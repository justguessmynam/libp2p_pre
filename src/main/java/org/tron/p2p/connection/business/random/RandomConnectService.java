package org.tron.p2p.connection.business.random;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect.DisconnectReason;

@Slf4j(topic = "net")
public class RandomConnectService {

  // 未配置时间基准时，初次连接时间的最大随机延迟。
  private static final long MAX_INITIAL_DELAY_MILLIS = 60_000L;

  private final TargetStateRepo targetStateRepo;
  private final DialTimePolicy dialTimePolicy;
  private final long cooldownMillis;

  public RandomConnectService(
      TargetStateRepo targetStateRepo,
      DialTimePolicy dialTimePolicy,
      long cooldownMillis) {
    if (targetStateRepo == null) {
      throw new IllegalArgumentException("targetStateRepo is null");
    }
    if (dialTimePolicy == null) {
      throw new IllegalArgumentException("dialTimePolicy is null");
    }
    if (cooldownMillis < 0) {
      throw new IllegalArgumentException(
          "cooldownMillis must not be negative");
    }

    this.targetStateRepo = targetStateRepo;
    this.dialTimePolicy = dialTimePolicy;
    this.cooldownMillis = cooldownMillis;
  }

  /**
   * 初始化全部active节点，并清除之前的运行状态。
   */
  public synchronized void initializeTargets(
      List<InetSocketAddress> activeNodes,
      Map<InetSocketAddress, Long> addressTimeMap) {
    targetStateRepo.clear();
    updateTargets(activeNodes, addressTimeMap);
  }

  /**
   * 根据activeNodes同步管理目标。
   *
   * <p>
   * addressTimeMap仅提供时间基准，不会扩展目标集合。
   */
  public synchronized void updateTargets(
      List<InetSocketAddress> activeNodes,
      Map<InetSocketAddress, Long> addressTimeMap) {
    Set<InetSocketAddress> validActiveNodes = validateActiveNodes(activeNodes);

    Map<InetSocketAddress, Long> timingMap = addressTimeMap == null
        ? Collections.emptyMap()
        : addressTimeMap;

    long now = System.currentTimeMillis();

    // activeNodes中已经删除的目标不再由本服务管理。
    removeInactiveTargets(validActiveNodes);

    // myAddressTimeMap中的非active节点只记录警告，不创建状态。
    warnUnusedTimingEntries(validActiveNodes, timingMap);

    for (InetSocketAddress address : validActiveNodes) {
      Long configuredBaseTime = getValidBaseTime(address, timingMap);

      TargetPeerState state = targetStateRepo.get(address);

      if (state == null) {
        // 有配置则使用配置时间，否则生成随机初始时间。
        long baseTimeMillis = configuredBaseTime != null
            ? configuredBaseTime
            : randomBaseTime(now);

        state = targetStateRepo.getOrCreate(
            new Node(address),
            baseTimeMillis);

        log.info(
            "Add random connect target {}, baseTime={}",
            address,
            baseTimeMillis);
      } else if (configuredBaseTime != null
          && state.getBaseTimeMillis() != configuredBaseTime) {
        // 只在配置明确提供新时间时更新。
        state.updateBaseTime(configuredBaseTime);

        log.info(
            "Update random connect target {}, baseTime={}",
            address,
            configuredBaseTime);
      }

      /*
       * 如果节点仍在activeNodes中，但时间配置被删除，
       * 则保留原有baseTimeMillis，与旧版行为一致。
       */
      refreshNextAttempt(state, now);
    }
  }

  /**
   * 判断地址是否由RandomConnectService管理。
   */
  public boolean isManagedTarget(InetSocketAddress address) {
    return targetStateRepo.contains(address);
  }

  /**
   * 获取当前已经到达连接时间的目标，并将其标记为正在拨号。
   */
  public List<TargetPeerState> claimDueTargets(long now) {
    List<TargetPeerState> dueTargets = new ArrayList<>();

    for (TargetPeerState state : targetStateRepo.all()) {
      synchronized (state) {
        if (state.canDialNow(now)) {
          state.markDialing(now);
          dueTargets.add(state);
        }
      }
    }

    return dueTargets;
  }

  /**
   * 记录TCP连接已经建立。
   */
  public void onChannelConnected(
      InetSocketAddress address,
      long eventTimeMillis) {
    TargetPeerState state = targetStateRepo.get(address);
    if (state == null) {
      return;
    }

    state.markChannelConnected(eventTimeMillis);
  }

  /**
   * 记录TRON握手已经完成。
   */
  public void onHandshakeComplete(
      InetSocketAddress address,
      long eventTimeMillis) {
    TargetPeerState state = targetStateRepo.get(address);
    if (state == null) {
      return;
    }

    state.markHandshakeComplete(eventTimeMillis);

    log.info(
        "Random connect handshake completed, target={}, time={}",
        address,
        eventTimeMillis);
  }

  /**
   * 记录连接失败，并根据冷却时间重新计算下次连接时间。
   */
  public void onConnectFailed(
      InetSocketAddress address,
      long eventTimeMillis) {
    TargetPeerState state = targetStateRepo.get(address);
    if (state == null) {
      return;
    }

    state.markConnectFailed(
        eventTimeMillis,
        cooldownMillis);

    refreshNextAttempt(state, eventTimeMillis);

    log.info(
        "Random connect failed, target={}, "
            + "cooldownUntil={}, nextAttemptAt={}",
        address,
        state.getCooldownUntil(),
        state.getNextAttemptAt());
  }

  /**
   * 记录连接断开及断开原因。
   */
  public void onDisconnected(
      InetSocketAddress address,
      DisconnectReason reason,
      long eventTimeMillis) {
    TargetPeerState state = targetStateRepo.get(address);
    if (state == null) {
      return;
    }

    DisconnectReason actualReason = reason == null
        ? DisconnectReason.UNKNOWN
        : reason;

    state.markDisconnected(
        eventTimeMillis,
        cooldownMillis,
        actualReason);

    /*
     * RANDOM_ELIMINATION发生时间将成为新的时间网格基准，
     * 用于计算下一轮更合适的连接时机。
     */
    if (actualReason == DisconnectReason.RANDOM_ELIMINATION) {
      state.markRandomElimination(eventTimeMillis);
    }

    refreshNextAttempt(state, eventTimeMillis);

    log.info(
        "Random connect disconnected, target={}, reason={}, "
            + "baseTime={}, cooldownUntil={}, nextAttemptAt={}",
        address,
        actualReason,
        state.getBaseTimeMillis(),
        state.getCooldownUntil(),
        state.getNextAttemptAt());
  }

  /**
   * 手动重新计算指定目标的下次连接时间。
   */
  public void refreshNextAttempt(InetSocketAddress address) {
    TargetPeerState state = targetStateRepo.get(address);
    if (state == null) {
      return;
    }

    refreshNextAttempt(
        state,
        System.currentTimeMillis());
  }

  public Collection<TargetPeerState> getTargetStates() {
    return targetStateRepo.allReadOnly();
  }

  public int getTargetCount() {
    return targetStateRepo.size();
  }

  private void refreshNextAttempt(
      TargetPeerState state,
      long now) {
    long nextAttemptAt = dialTimePolicy.nextAttemptTime(
        state.getBaseTimeMillis(),
        now,
        state.getCooldownUntil(),
        state.getLastAttemptAt());

    state.updateNextAttemptAt(nextAttemptAt);
  }

  /**
   * 删除已经不在activeNodes中的目标。
   */
  private void removeInactiveTargets(
      Set<InetSocketAddress> activeNodes) {
    for (TargetPeerState state : targetStateRepo.all()) {
      if (activeNodes.contains(state.getAddress())) {
        continue;
      }

      targetStateRepo.remove(state.getAddress());

      log.info(
          "Remove random connect target {}",
          state.getAddress());
    }
  }

  /**
   * 校验并去重activeNodes。
   */
  private Set<InetSocketAddress> validateActiveNodes(
      List<InetSocketAddress> activeNodes) {
    Set<InetSocketAddress> validNodes = new LinkedHashSet<>();

    if (activeNodes == null) {
      return validNodes;
    }

    for (InetSocketAddress address : activeNodes) {
      if (!isValidAddress(address)) {
        log.warn(
            "Ignore invalid active random connect target, "
                + "address={}",
            address);
        continue;
      }

      validNodes.add(address);
    }

    return validNodes;
  }

  /**
   * 提醒配置中存在不属于activeNodes的时间条目。
   */
  private void warnUnusedTimingEntries(
      Set<InetSocketAddress> activeNodes,
      Map<InetSocketAddress, Long> addressTimeMap) {
    for (Map.Entry<InetSocketAddress, Long> entry : addressTimeMap.entrySet()) {
      if (activeNodes.contains(entry.getKey())) {
        continue;
      }

      log.warn(
          "Ignore random connect timing entry because "
              + "target is not active, address={}",
          entry.getKey());
    }
  }

  /**
   * 读取有效的时间基准；未配置或配置无效时返回null。
   */
  private Long getValidBaseTime(
      InetSocketAddress address,
      Map<InetSocketAddress, Long> addressTimeMap) {
    Long baseTimeMillis = addressTimeMap.get(address);

    if (baseTimeMillis == null) {
      return null;
    }

    if (baseTimeMillis <= 0) {
      log.warn(
          "Ignore invalid random connect base time, "
              + "address={}, baseTime={}",
          address,
          baseTimeMillis);
      return null;
    }

    return baseTimeMillis;
  }

  private boolean isValidAddress(
      InetSocketAddress address) {
    return address != null
        && address.getAddress() != null
        && address.getPort() > 0;
  }

  /**
   * 为没有时间配置的active节点生成初始时间基准。
   */
  private long randomBaseTime(long now) {
    long randomDelay = ThreadLocalRandom.current().nextLong(
        1,
        MAX_INITIAL_DELAY_MILLIS + 1);

    return now + randomDelay;
  }
}