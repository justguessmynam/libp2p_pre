package org.tron.p2p.connection.business.handshake;

import static org.tron.p2p.connection.ChannelManager.getDisconnectReason;
import static org.tron.p2p.connection.ChannelManager.logDisconnectReason;

import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.connection.Channel;
import org.tron.p2p.connection.ChannelManager;
import org.tron.p2p.connection.business.MessageProcess;
import org.tron.p2p.connection.message.Message;
import org.tron.p2p.connection.message.base.P2pDisconnectMessage;
import org.tron.p2p.connection.message.handshake.HelloMessage;
import org.tron.p2p.protos.Connect.DisconnectReason;

@Slf4j(topic = "net")
public class HandshakeService implements MessageProcess {

  private final int networkId = Parameter.p2pConfig.getNetworkId();

  public void startHandshake(Channel channel) {
    sendHelloMsg(channel, DisconnectCode.NORMAL, channel.getStartTime());
  }

  @Override
  public void processMessage(Channel channel, Message message) {
    HelloMessage msg = (HelloMessage) message;

    if (channel.isFinishHandshake()) {
      log.warn("Close channel {}, handshake is finished", channel.getInetSocketAddress());
      channel.send(new P2pDisconnectMessage(DisconnectReason.DUP_HANDSHAKE));
      channel.close();
      return;
    }

    channel.setHelloMessage(msg);

    DisconnectCode code = ChannelManager.processPeer(channel);
    if (code != DisconnectCode.NORMAL) {
      DisconnectReason reason = getDisconnectReason(code);

      if (!channel.isActive()) {
        sendHelloMsg(channel, code, msg.getTimestamp());
      }

      channel.recordLocalDisconnectReason(reason);
      logDisconnectReason(channel, getDisconnectReason(code));
      channel.close();
      return;
    }

    ChannelManager.updateNodeId(channel, msg.getFrom().getHexId());
    if (channel.isDisconnect()) {
      return;
    }

    if (channel.isActive()) {
      boolean remoteRejected = msg.getCode() != DisconnectCode.NORMAL.getValue();
      boolean incompatibleNetwork = msg.getNetworkId() != networkId
          && msg.getVersion() != networkId;

      if (remoteRejected || incompatibleNetwork) {
        DisconnectCode disconnectCode = DisconnectCode.forNumber(msg.getCode());

        DisconnectReason reason;
        if (remoteRejected && disconnectCode != null) {
          reason = getDisconnectReason(disconnectCode);
          channel.recordRemoteDisconnectReason(reason);
        } else {
          reason = DisconnectReason.DIFFERENT_VERSION;
          channel.recordLocalDisconnectReason(reason);
        }

        log.info(
            "Handshake failed {}, code: {}, reason: {}, networkId: {}, version: {}",
            channel.getInetSocketAddress(),
            msg.getCode(),
            reason,
            msg.getNetworkId(),
            msg.getVersion());

        logDisconnectReason(channel, reason);
        channel.close();
        return;
      }
    } else {

      if (msg.getNetworkId() != networkId) {
        log.info("Peer {} different network id, peer->{}, me->{}",
            channel.getInetSocketAddress(), msg.getNetworkId(), networkId);
        sendHelloMsg(channel, DisconnectCode.DIFFERENT_VERSION, msg.getTimestamp());
        channel.recordLocalDisconnectReason(DisconnectReason.DIFFERENT_VERSION);
        logDisconnectReason(channel, DisconnectReason.DIFFERENT_VERSION);
        channel.close();
        return;
      }
      sendHelloMsg(channel, DisconnectCode.NORMAL, msg.getTimestamp());
    }
    channel.setFinishHandshake(true);

    /*
     * 必须在确认网络、版本、节点ID均有效之后，
     * 才能将目标标记为握手成功。
     */
    ChannelManager.notifyHandshakeComplete(channel);

    channel.updateAvgLatency(System.currentTimeMillis() - channel.getStartTime());
    Parameter.handlerList.forEach(h -> h.onConnect(channel));
  }

  private void sendHelloMsg(Channel channel, DisconnectCode code, long time) {
    HelloMessage helloMessage = new HelloMessage(code, time);
    channel.send(helloMessage);
  }

}
