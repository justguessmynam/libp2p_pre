package org.tron.p2p;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.connection.Channel;
import org.tron.p2p.connection.ChannelManager;
import org.tron.p2p.connection.business.random.RandomConnectService;
import org.tron.p2p.discover.Node;
import org.tron.p2p.discover.NodeManager;
import org.tron.p2p.dns.DnsManager;
import org.tron.p2p.exception.P2pException;
import org.tron.p2p.stats.P2pStats;
import org.tron.p2p.stats.StatsManager;

@Slf4j(topic = "net")
public class P2pService {

  private StatsManager statsManager = new StatsManager();
  private volatile boolean isShutdown = false;

  public void start(P2pConfig p2pConfig) {
    Parameter.p2pConfig = p2pConfig;
    NodeManager.init();
    ChannelManager.init();
    DnsManager.init();
    log.info("P2p service started");

    Runtime.getRuntime().addShutdownHook(new Thread(this::close));
  }

  public void close() {
    if (isShutdown) {
      return;
    }
    isShutdown = true;
    DnsManager.close();
    NodeManager.close();
    ChannelManager.close();
    log.info("P2p service closed");
  }

  public void register(P2pEventHandler p2PEventHandler) throws P2pException {
    Parameter.addP2pEventHandle(p2PEventHandler);
  }

  @Deprecated
  public void connect(InetSocketAddress address) {
    ChannelManager.connect(address);
  }

  public ChannelFuture connect(Node node, ChannelFutureListener future) {
    return ChannelManager.connect(node, future);
  }

  /**
   * Updates active targets and their optional connection-time baselines.
   */
  public synchronized void updateRandomConnectTargets(
      List<InetSocketAddress> activeNodes,
      Map<InetSocketAddress, Long> addressTimeMap) {
    if (isShutdown) {
      log.warn("Ignore random connect target update because P2pService is shutdown");
      return;
    }

    P2pConfig p2pConfig = Parameter.p2pConfig;
    RandomConnectService randomConnectService = ChannelManager.getRandomConnectService();
    if (p2pConfig == null || randomConnectService == null) {
      log.warn("Ignore random connect target update because P2pService is not initialized");
      return;
    }

    List<InetSocketAddress> newActiveNodes = normalizeActiveNodes(activeNodes);
    Map<InetSocketAddress, Long> newAddressTimeMap =
        normalizeAddressTimeMap(addressTimeMap);

    // Manage the old and new targets during the configuration transition.
    Set<InetSocketAddress> transitionTargets = new LinkedHashSet<>();
    if (p2pConfig.getActiveNodes() != null) {
      transitionTargets.addAll(p2pConfig.getActiveNodes());
    }
    transitionTargets.addAll(newActiveNodes);

    randomConnectService.updateTargets(
        new ArrayList<>(transitionTargets), newAddressTimeMap);

    // Replace the collections atomically instead of exposing a temporary empty state.
    p2pConfig.setActiveNodes(new CopyOnWriteArrayList<>(newActiveNodes));
    p2pConfig.setMyAddressTimeMap(new ConcurrentHashMap<>(newAddressTimeMap));

    // Remove targets that are no longer active after the configuration is visible.
    randomConnectService.updateTargets(newActiveNodes, newAddressTimeMap);

    log.info(
        "Updated random connect targets, activeNodes={}, timingEntries={}",
        newActiveNodes.size(),
        newAddressTimeMap.size());
  }

  private List<InetSocketAddress> normalizeActiveNodes(
      List<InetSocketAddress> activeNodes) {
    Set<InetSocketAddress> normalized = new LinkedHashSet<>();
    if (activeNodes != null) {
      for (InetSocketAddress address : activeNodes) {
        if (address != null) {
          normalized.add(address);
        }
      }
    }
    return new ArrayList<>(normalized);
  }

  private Map<InetSocketAddress, Long> normalizeAddressTimeMap(
      Map<InetSocketAddress, Long> addressTimeMap) {
    Map<InetSocketAddress, Long> normalized = new LinkedHashMap<>();
    if (addressTimeMap == null) {
      return normalized;
    }

    for (Map.Entry<InetSocketAddress, Long> entry : addressTimeMap.entrySet()) {
      InetSocketAddress address = entry.getKey();
      Long baseTimeMillis = entry.getValue();
      if (address != null && baseTimeMillis != null) {
        normalized.put(address, baseTimeMillis);
      }
    }
    return normalized;
  }

  public P2pStats getP2pStats() {
    return statsManager.getP2pStats();
  }

  public List<Node> getTableNodes() {
    return NodeManager.getTableNodes();
  }

  public List<Node> getConnectableNodes() {
    Set<Node> nodes = new HashSet<>();
    nodes.addAll(NodeManager.getConnectableNodes());
    nodes.addAll(DnsManager.getDnsNodes());
    return new ArrayList<>(nodes);
  }

  public List<Node> getAllNodes() {
    Set<Node> nodes = new HashSet<>();
    nodes.addAll(NodeManager.getAllNodes());
    nodes.addAll(DnsManager.getDnsNodes());
    return new ArrayList<>(nodes);
  }

  public void updateNodeId(Channel channel, String nodeId) {
    ChannelManager.updateNodeId(channel, nodeId);
  }

  public int getVersion() {
    return Parameter.version;
  }
}
