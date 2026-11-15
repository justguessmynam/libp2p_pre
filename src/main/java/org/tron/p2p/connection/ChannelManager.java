package org.tron.p2p.connection;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.encoders.Hex;
import org.tron.p2p.P2pEventHandler;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.connection.business.detect.NodeDetectService;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.connection.business.handshake.HandshakeService;
import org.tron.p2p.connection.business.keepalive.KeepAliveService;
import org.tron.p2p.connection.business.pool.ConnPoolService;
import org.tron.p2p.connection.business.random.DialTimePolicy;
import org.tron.p2p.connection.business.random.RandomConnectService;
import org.tron.p2p.connection.business.random.TargetStateRepo;
import org.tron.p2p.connection.message.Message;
import org.tron.p2p.connection.message.MessageType;
import org.tron.p2p.connection.message.base.P2pDisconnectMessage;
import org.tron.p2p.connection.socket.PeerClient;
import org.tron.p2p.connection.socket.PeerServer;
import org.tron.p2p.discover.Node;
import org.tron.p2p.exception.P2pException;
import org.tron.p2p.exception.P2pException.TypeEnum;
import org.tron.p2p.protos.Connect.DisconnectReason;
import org.tron.p2p.utils.ByteArray;
import org.tron.p2p.utils.NetUtil;

@Slf4j(topic = "net")
public class ChannelManager {

  // 相邻随机连接时机的时间网格。
  private static final long RANDOM_CONNECT_GRID_MILLIS = 30_000L;

  // 同一个目标两次连接尝试的最小间隔。
  private static final long RANDOM_CONNECT_MIN_ATTEMPT_INTERVAL_MILLIS = 60_000L;

  // 计算连接时机时要求保留的最小提前量。
  private static final long RANDOM_CONNECT_MIN_LEAD_MILLIS = 200L;

  // 连接失败或断开后的冷却时间。
  private static final long RANDOM_CONNECT_COOLDOWN_MILLIS = 60_000L;

  // 检查目标连接时机的扫描间隔。
  private static final long RANDOM_CONNECT_SCAN_INTERVAL_MILLIS = 50L;

  @Getter
  private static NodeDetectService nodeDetectService;

  private static PeerServer peerServer;

  @Getter
  private static RandomConnectService randomConnectService;

  @Getter
  private static PeerClient peerClient;

  @Getter
  private static ConnPoolService connPoolService;

  private static KeepAliveService keepAliveService;

  @Getter
  private static HandshakeService handshakeService;

  @Getter
  private static final Map<InetSocketAddress, Channel> channels = new ConcurrentHashMap<>();

  @Getter
  private static final Cache<InetAddress, Long> bannedNodes = CacheBuilder
      .newBuilder().maximumSize(2000).build(); // ban timestamp

  private static boolean isInit = false;
  public static volatile boolean isShutdown = false;

  public static void init() {
    isInit = true;
    peerServer = new PeerServer();
    peerClient = new PeerClient();
    keepAliveService = new KeepAliveService();
    connPoolService = new ConnPoolService();
    handshakeService = new HandshakeService();
    nodeDetectService = new NodeDetectService();
    peerServer.init();
    peerClient.init();

    randomConnectService = new RandomConnectService(
        peerClient,
        new TargetStateRepo(),
        new DialTimePolicy(
            RANDOM_CONNECT_GRID_MILLIS,
            RANDOM_CONNECT_MIN_ATTEMPT_INTERVAL_MILLIS,
            RANDOM_CONNECT_MIN_LEAD_MILLIS),
        RANDOM_CONNECT_COOLDOWN_MILLIS,
        RANDOM_CONNECT_SCAN_INTERVAL_MILLIS);

    randomConnectService.initializeTargets(
        Parameter.p2pConfig.getActiveNodes(),
        Parameter.p2pConfig.getMyAddressTimeMap());

    randomConnectService.start();

    keepAliveService.init();
    connPoolService.init(peerClient);
    nodeDetectService.init(peerClient);
  }

  /**
   * 返回channel对应的RandomConnectService目标地址。
   */
  private static InetSocketAddress getManagedTargetAddress(
      Channel channel) {
    if (channel == null || randomConnectService == null) {
      return null;
    }

    /*
     * 对于本节点主动建立的连接，socket远端地址通常就是
     * activeNodes中配置的目标地址。
     */
    InetSocketAddress socketAddress = channel.getInetSocketAddress();

    if (randomConnectService.isManagedTarget(socketAddress)) {
      return socketAddress;
    }

    /*
     * 对方主动连接本节点时，TCP来源端口可能是临时端口。
     * 此时使用握手消息中声明的P2P服务地址进行匹配。
     */
    Node remoteNode = channel.getNode();

    if (remoteNode == null) {
      return null;
    }

    InetSocketAddress advertisedAddress = remoteNode.getPreferInetSocketAddress();

    if (randomConnectService.isManagedTarget(advertisedAddress)) {
      return advertisedAddress;
    }

    return null;
  }

  /**
   * 将握手完成事件通知给受管目标。
   */
  public static void notifyHandshakeComplete(Channel channel) {
    InetSocketAddress targetAddress = getManagedTargetAddress(channel);

    if (targetAddress == null) {
      return;
    }

    randomConnectService.onHandshakeComplete(
        targetAddress,
        System.currentTimeMillis());
  }

  public static void connect(InetSocketAddress address) {
    peerClient.connect(address.getAddress().getHostAddress(), address.getPort(),
        ByteArray.toHexString(NetUtil.getNodeId()));
  }

  public static ChannelFuture connect(Node node, ChannelFutureListener future) {
    return peerClient.connect(node, future);
  }

  public static void notifyDisconnect(Channel channel) {
    /*
     * 在channel从连接表删除之前，直接从当前业务Channel中
     * 读取目标地址和断开原因，避免关闭后再次查询失败。
     */
    InetSocketAddress targetAddress = getManagedTargetAddress(channel);

    if (targetAddress != null) {
      randomConnectService.onDisconnected(
          targetAddress,
          channel.getDisconnectReason(),
          System.currentTimeMillis());
    }

    InetSocketAddress socketAddress = channel.getInetSocketAddress();

    if (socketAddress == null) {
      log.warn("Notify Disconnect peer has no address.");
      return;
    }

    channels.remove(socketAddress);

    Parameter.handlerList.forEach(
        handler -> handler.onDisconnect(channel));

    InetAddress inetAddress = channel.getInetAddress();
    if (inetAddress != null) {
      banNode(
          inetAddress,
          Parameter.DEFAULT_BAN_TIME);
    }
  }

  public static int getConnectionNum(InetAddress inetAddress) {
    int cnt = 0;
    for (Channel channel : channels.values()) {
      if (channel.getInetAddress().equals(inetAddress)) {
        cnt++;
      }
    }
    return cnt;
  }

  public static synchronized DisconnectCode processPeer(Channel channel) {

    if (!channel.isActive() && !channel.isTrustPeer()) {
      InetAddress inetAddress = channel.getInetAddress();
      if (bannedNodes.getIfPresent(inetAddress) != null
          && bannedNodes.getIfPresent(inetAddress) > System.currentTimeMillis()) {
        log.info("Peer {} recently disconnected", channel);
        return DisconnectCode.TIME_BANNED;
      }

      if (channels.size() >= Parameter.p2pConfig.getMaxConnections()) {
        log.info("Too many peers, disconnected with {}", channel);
        return DisconnectCode.TOO_MANY_PEERS;
      }

      int num = getConnectionNum(channel.getInetAddress());
      if (num >= Parameter.p2pConfig.getMaxConnectionsWithSameIp()) {
        log.info("Max connection with same ip {}", channel);
        return DisconnectCode.MAX_CONNECTION_WITH_SAME_IP;
      }
    }

    if (StringUtils.isNotEmpty(channel.getNodeId())) {
      for (Channel c : channels.values()) {
        if (channel.getNodeId().equals(c.getNodeId())) {
          if (c.getStartTime() > channel.getStartTime()) {
            c.close();
          } else {
            log.info("Duplicate peer {}, exist peer {}", channel, c);
            return DisconnectCode.DUPLICATE_PEER;
          }
        }
      }
    }

    channels.put(channel.getInetSocketAddress(), channel);

    log.info("Add peer {}, total channels: {}", channel.getInetSocketAddress(), channels.size());
    return DisconnectCode.NORMAL;
  }

  public static DisconnectReason getDisconnectReason(DisconnectCode code) {
    DisconnectReason disconnectReason;
    switch (code) {
      case DIFFERENT_VERSION:
        disconnectReason = DisconnectReason.DIFFERENT_VERSION;
        break;
      case TIME_BANNED:
        disconnectReason = DisconnectReason.RECENT_DISCONNECT;
        break;
      case DUPLICATE_PEER:
        disconnectReason = DisconnectReason.DUPLICATE_PEER;
        break;
      case TOO_MANY_PEERS:
        disconnectReason = DisconnectReason.TOO_MANY_PEERS;
        break;
      case MAX_CONNECTION_WITH_SAME_IP:
        disconnectReason = DisconnectReason.TOO_MANY_PEERS_WITH_SAME_IP;
        break;
      default: {
        disconnectReason = DisconnectReason.UNKNOWN;
      }
    }
    return disconnectReason;
  }

  public static void logDisconnectReason(Channel channel, DisconnectReason reason) {
    log.info("Try to close channel: {}, reason: {}", channel.getInetSocketAddress(), reason.name());
  }

  public static void banNode(InetAddress inetAddress, Long banTime) {
    long now = System.currentTimeMillis();
    if (bannedNodes.getIfPresent(inetAddress) == null
        || bannedNodes.getIfPresent(inetAddress) < now) {
      bannedNodes.put(inetAddress, now + banTime);
    }
  }

  public static void close() {
    if (!isInit || isShutdown) {
      return;
    }
    isShutdown = true;
    /*
     * 必须先停止随机连接扫描，避免PeerClient关闭过程中
     * 仍然产生新的连接请求。
     */
    if (randomConnectService != null) {
      randomConnectService.stop();
    }
    connPoolService.close();
    keepAliveService.close();
    peerServer.close();
    peerClient.close();
    nodeDetectService.close();
  }

  public static void processMessage(Channel channel, byte[] data) throws P2pException {
    if (data == null || data.length == 0) {
      throw new P2pException(TypeEnum.EMPTY_MESSAGE, "");
    }
    if (data[0] >= 0) {
      handMessage(channel, data);
      return;
    }

    Message message = Message.parse(data);

    if (message.needToLog()) {
      log.info("Receive message from channel: {}, {}", channel.getInetSocketAddress(), message);
    } else {
      log.debug("Receive message from channel {}, {}", channel.getInetSocketAddress(), message);
    }

    if (channel.isDiscoveryMode() && message.getType() != MessageType.STATUS) {
      log.debug("Discovery channel {} received unexpected message {}, close it",
          channel.getInetSocketAddress(), message.getType());
      channel.close();
      return;
    }

    switch (message.getType()) {
      case KEEP_ALIVE_PING:
      case KEEP_ALIVE_PONG:
        keepAliveService.processMessage(channel, message);
        break;
      case HANDSHAKE_HELLO:
        handshakeService.processMessage(channel, message);
        break;
      case STATUS:
        nodeDetectService.processMessage(channel, message);
        break;
      case DISCONNECT:
        P2pDisconnectMessage disconnectMessage = (P2pDisconnectMessage) message;
        channel.recordRemoteDisconnectReason(disconnectMessage.getReason());
        channel.close();
        break;
      default:
        throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, "type:" + data[0]);
    }
  }

  private static void handMessage(Channel channel, byte[] data) throws P2pException {
    P2pEventHandler handler = Parameter.handlerMap.get(data[0]);
    if (handler == null) {
      throw new P2pException(P2pException.TypeEnum.NO_SUCH_MESSAGE, "type:" + data[0]);
    }
    if (channel.isDiscoveryMode()) {
      channel.send(new P2pDisconnectMessage(DisconnectReason.DISCOVER_MODE));
      channel.getCtx().close();
      return;
    }

    if (!channel.isFinishHandshake()) {
      channel.setFinishHandshake(true);

      DisconnectCode code = processPeer(channel);
      if (!DisconnectCode.NORMAL.equals(code)) {
        DisconnectReason disconnectReason = getDisconnectReason(code);

        channel.send(
            new P2pDisconnectMessage(disconnectReason));

        channel.getCtx().close();
        return;
      }

      notifyHandshakeComplete(channel);

      Parameter.handlerList.forEach(
          eventHandler -> eventHandler.onConnect(channel));
    }

    handler.onMessage(channel, data);
  }

  public static synchronized void updateNodeId(Channel channel, String nodeId) {
    channel.setNodeId(nodeId);
    if (nodeId.equals(Hex.toHexString(Parameter.p2pConfig.getNodeID()))) {
      log.warn("Channel {} is myself", channel.getInetSocketAddress());
      channel.send(new P2pDisconnectMessage(DisconnectReason.DUPLICATE_PEER));
      channel.close();
      return;
    }

    List<Channel> list = new ArrayList<>();
    channels.values().forEach(c -> {
      if (nodeId.equals(c.getNodeId())) {
        list.add(c);
      }
    });
    if (list.size() <= 1) {
      return;
    }
    Channel c1 = list.get(0);
    Channel c2 = list.get(1);
    if (c1.getStartTime() > c2.getStartTime()) {
      log.info("Close channel {}, other channel {} is earlier", c1, c2);
      c1.send(new P2pDisconnectMessage(DisconnectReason.DUPLICATE_PEER));
      c1.close();
    } else {
      log.info("Close channel {}, other channel {} is earlier", c2, c1);
      c2.send(new P2pDisconnectMessage(DisconnectReason.DUPLICATE_PEER));
      c2.close();
    }
  }

  public static void triggerConnect(InetSocketAddress address) {
    connPoolService.triggerConnect(address);
  }
}
