package org.tron.p2p.connection.business.random;

import java.net.InetSocketAddress;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.tron.p2p.connection.socket.PeerClient;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect.DisconnectReason;

@Slf4j(topic = "RANDOM_CONNECT")
public class RandomConnectService {

  // 未配置时间基准时，初次连接时间的最大随机延迟。
  private static final long MAX_INITIAL_DELAY_MILLIS = 60_000L;

  // 拨号超过该时间仍未产生结果时，释放dialing状态。
  private static final long DIALING_TIMEOUT_MILLIS = 15_000L;

  // 随机连接目标状态的统计日志间隔。
  private static final long STATS_INTERVAL_SECONDS = 150L;

  private final PeerClient peerClient;
  private final TargetStateRepo targetStateRepo;
  private final DialTimePolicy dialTimePolicy;
  private final long cooldownMillis;
  private final long scanIntervalMillis;

  // 单线程扫描，避免同一批目标被并发调度。
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      BasicThreadFactory.builder()
          .namingPattern("random-connect-%d")
          .build());

  // 独立输出统计日志，避免影响高频连接扫描。
  private final ScheduledExecutorService statsScheduler = Executors.newSingleThreadScheduledExecutor(
      BasicThreadFactory.builder()
          .namingPattern("random-connect-stats-%d")
          .build());

  private volatile boolean started;

  public RandomConnectService(
      PeerClient peerClient,
      TargetStateRepo targetStateRepo,
      DialTimePolicy dialTimePolicy,
      long cooldownMillis,
      long scanIntervalMillis) {
    if (peerClient == null) {
      throw new IllegalArgumentException("peerClient is null");
    }
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
    if (scanIntervalMillis <= 0) {
      throw new IllegalArgumentException(
          "scanIntervalMillis must be greater than 0");
    }

    this.peerClient = peerClient;
    this.targetStateRepo = targetStateRepo;
    this.dialTimePolicy = dialTimePolicy;
    this.cooldownMillis = cooldownMillis;
    this.scanIntervalMillis = scanIntervalMillis;
  }

  /**
   * 启动随机连接目标扫描任务。
   */
  public synchronized void start() {
    if (started) {
      return;
    }

    started = true;

    scheduler.scheduleAtFixedRate(
        this::safeScanAndConnect,
        0,
        scanIntervalMillis,
        TimeUnit.MILLISECONDS);

    statsScheduler.scheduleAtFixedRate(
        this::safeLogRandomStats,
        0,
        STATS_INTERVAL_SECONDS,
        TimeUnit.SECONDS);

    log.info(
        "RandomConnectService started, scanInterval={}ms",
        scanIntervalMillis);
  }

  /**
   * 停止随机连接目标扫描任务。
   */
  public synchronized void stop() {
    if (!started) {
      return;
    }

    started = false;
    scheduler.shutdownNow();
    statsScheduler.shutdownNow();

    log.info("RandomConnectService stopped");
  }

  public boolean isStarted() {
    return started;
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

  /**
   * 返回已完成TRON握手的目标数量。
   */
  public int getConnectedTargetCount() {
    int count = 0;
    for (TargetPeerState state : targetStateRepo.all()) {
      if (state.isHandshakeComplete()) {
        count++;
      }
    }
    return count;
  }

  /**
   * 返回正在拨号或等待TRON握手的目标数量。
   */
  public int getPendingTargetCount() {
    int count = 0;
    for (TargetPeerState state : targetStateRepo.all()) {
      if (state.isDialing()
          || (state.isChannelConnected() && !state.isHandshakeComplete())) {
        count++;
      }
    }
    return count;
  }

  /**
   * 返回已占用连接流程的目标数量。
   */
  public int getOccupiedTargetCount() {
    int count = 0;
    for (TargetPeerState state : targetStateRepo.all()) {
      if (state.isDialing()
          || state.isChannelConnected()
          || state.isHandshakeComplete()) {
        count++;
      }
    }
    return count;
  }

  /**
   * 输出统计日志时隔离异常，避免定时任务永久停止。
   */
  private void safeLogRandomStats() {
    if (!started) {
      return;
    }

    try {
      logRandomStats();
    } catch (Throwable t) {
      log.error("Random connect stats failed", t);
    }
  }

  /**
   * 汇总目标运行状态，并保留旧版按IP划分的连接统计日志。
   */
  private void logRandomStats() {
    int handshakeComplete = 0;
    int tcpPendingHandshake = 0;
    int dialing = 0;
    int idle = 0;
    int randomElimination = 0;

    Set<String> connectedIps = new TreeSet<>();
    Set<String> connectedRandomEliIps = new TreeSet<>();
    Set<String> unconnectedIps = new TreeSet<>();
    Set<String> unconnectedRandomEliIps = new TreeSet<>();

    Collection<TargetPeerState> states = targetStateRepo.all();
    for (TargetPeerState state : states) {
      boolean connected;
      boolean eliminated;

      synchronized (state) {
        connected = state.isHandshakeComplete();
        eliminated = state.getLastRandomEliminationAt() > 0;

        if (connected) {
          handshakeComplete++;
        } else if (state.isDialing()) {
          dialing++;
        } else if (state.isChannelConnected()) {
          tcpPendingHandshake++;
        } else {
          idle++;
        }

        if (eliminated) {
          randomElimination++;
        }
      }

      String ip = targetIp(state);
      if (ip == null) {
        continue;
      }

      if (connected) {
        connectedIps.add(ip);
        if (eliminated) {
          connectedRandomEliIps.add(ip);
        }
      } else {
        unconnectedIps.add(ip);
        if (eliminated) {
          unconnectedRandomEliIps.add(ip);
        }
      }
    }

    log.info(
        "RANDOM_TARGET_STATS total={}, handshakeComplete={}, "
            + "tcpPendingHandshake={}, dialing={}, idle={}, randomElimination={}",
        states.size(),
        handshakeComplete,
        tcpPendingHandshake,
        dialing,
        idle,
        randomElimination);

    log.info(
        "RANDOM_CONNECTED total={}, ips={}, randomEliIps={}",
        connectedIps.size(),
        connectedIps,
        connectedRandomEliIps);

    log.info(
        "RANDOM_UNCONNECTED total={}, ips={}, randomEliIps={}",
        unconnectedIps.size(),
        unconnectedIps,
        unconnectedRandomEliIps);
  }

  private String targetIp(TargetPeerState state) {
    InetSocketAddress address = state == null ? null : state.getAddress();
    if (address == null || address.getAddress() == null) {
      return null;
    }
    return address.getAddress().getHostAddress();
  }

  /**
   * 执行扫描时隔离异常，避免定时任务因一次异常永久停止。
   */
  private void safeScanAndConnect() {
    if (!started) {
      return;
    }

    try {
      scanAndConnect();
    } catch (Throwable t) {
      log.error("Random connect scan failed", t);
    }
  }

  /**
   * 清理超时拨号，然后领取并连接已经到期的目标。
   */
  private void scanAndConnect() {
    long now = System.currentTimeMillis();

    cleanupStaleDialing(now);

    for (TargetPeerState state : claimDueTargets(now)) {
      doConnect(state);
    }
  }

  /**
   * 清除长时间没有完成的拨号状态。
   */
  private void cleanupStaleDialing(long now) {
    for (TargetPeerState state : targetStateRepo.all()) {
      synchronized (state) {
        if (!state.isDialing()) {
          continue;
        }

        long lastAttemptAt = state.getLastAttemptAt();

        if (lastAttemptAt <= 0
            || now - lastAttemptAt <= DIALING_TIMEOUT_MILLIS) {
          continue;
        }

        log.warn(
            "Reset stale random connect dialing, target={}, "
                + "lastAttemptAt={}",
            state.getAddress(),
            lastAttemptAt);

        /*
         * 此时仍持有state锁，避免连接成功回调和超时清理
         * 同时修改同一个目标的状态。
         */
        onConnectFailed(state.getAddress(), now);
      }
    }
  }

  /**
   * 使用官方PeerClient接口发起连接。
   */
  private void doConnect(TargetPeerState state) {
    Node node = state.getNode();
    InetSocketAddress address = state.getAddress();

    log.info("Random connect to {}", address);

    try {
      ChannelFuture future = peerClient.connect(
          node,
          (ChannelFutureListener) result -> handleConnectResult(address, result));

      /*
       * ChannelManager关闭时PeerClient可能返回null，
       * 此时释放dialing状态并等待下一轮调度。
       */
      if (future == null) {
        onConnectFailed(
            address,
            System.currentTimeMillis());
      }
    } catch (RuntimeException e) {
      log.warn(
          "Random connect request failed, peer={}, cause={}",
          address,
          e.getMessage());

      onConnectFailed(
          address,
          System.currentTimeMillis());
    }
  }

  /**
   * 处理Netty TCP连接结果。
   */
  private void handleConnectResult(
      InetSocketAddress address,
      ChannelFuture future) {
    long eventTimeMillis = System.currentTimeMillis();

    if (future.isSuccess()) {
      onChannelConnected(address, eventTimeMillis);

      log.info(
          "Random connect channel established, target={}, time={}",
          address,
          eventTimeMillis);

      return;
    }

    Throwable cause = future.cause();

    log.warn(
        "Random connect failed, peer={}, cause={}",
        address,
        cause == null ? "unknown" : cause.getMessage());

    onConnectFailed(address, eventTimeMillis);

    /*
     * 连接失败后关闭Netty channel，释放相关资源。
     */
    if (future.channel() != null) {
      future.channel().close();
    }
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