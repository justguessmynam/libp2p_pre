package org.tron.p2p.connection.business.random;

import java.net.InetSocketAddress;
import lombok.Getter;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect.DisconnectReason;

@Getter
public class TargetPeerState {

  // Stable configured address used as the repository key.
  private final InetSocketAddress address;

  // libp2p node used when opening the connection.
  private final Node node;

  // Reference time used to calculate the connection time grid.
  private volatile long baseTimeMillis;

  // Next scheduled connection attempt time.
  private volatile long nextAttemptAt;

  // True while a connection attempt is in progress.
  private volatile boolean dialing;

  // True after the TCP/Netty channel has been established.
  private volatile boolean channelConnected;

  // True only after the TRON handshake has completed.
  private volatile boolean handshakeComplete;

  // Time when the latest connection attempt started.
  private volatile long lastAttemptAt;

  // No connection attempt is allowed before this time.
  private volatile long cooldownUntil;

  // Time when the latest channel connection succeeded.
  private volatile long lastChannelConnectedAt;

  // Time when the latest TRON handshake completed.
  private volatile long lastConnectedAt;

  // Time when the latest connection closed.
  private volatile long lastDisconnectedAt;

  // Time of the latest RANDOM_ELIMINATION disconnect.
  private volatile long lastRandomEliminationAt;

  // Most recently observed libp2p disconnect reason.
  private volatile DisconnectReason lastDisconnectReason = DisconnectReason.UNKNOWN;

  public TargetPeerState(Node node, long baseTimeMillis) {
    if (node == null) {
      throw new IllegalArgumentException("node is null");
    }

    InetSocketAddress targetAddress = node.getPreferInetSocketAddress();
    if (targetAddress == null) {
      throw new IllegalArgumentException("node address is null");
    }

    this.address = targetAddress;
    this.node = node;
    this.baseTimeMillis = baseTimeMillis;
  }

  public boolean isInCooldown(long now) {
    return now < cooldownUntil;
  }

  public boolean canDialByState(long now) {
    return !dialing
        && !channelConnected
        && !handshakeComplete
        && now >= cooldownUntil;
  }

  public boolean isAttemptDue(long now) {
    return nextAttemptAt > 0 && now >= nextAttemptAt;
  }

  public boolean canDialNow(long now) {
    return canDialByState(now) && isAttemptDue(now);
  }

  public synchronized void markDialing(long now) {
    dialing = true;
    lastAttemptAt = now;
  }

  public synchronized void markChannelConnected(long now) {
    dialing = false;
    channelConnected = true;
    lastChannelConnectedAt = now;
  }

  public synchronized void markHandshakeComplete(long now) {
    dialing = false;
    channelConnected = true;
    handshakeComplete = true;
    lastConnectedAt = now;
  }

  public synchronized void markConnectFailed(long now, long cooldownMillis) {
    resetConnectionState();
    cooldownUntil = now + cooldownMillis;
  }

  public synchronized void markDisconnected(
      long now, long cooldownMillis, DisconnectReason reason) {
    resetConnectionState();
    cooldownUntil = now + cooldownMillis;
    lastDisconnectedAt = now;
    lastDisconnectReason = reason == null ? DisconnectReason.UNKNOWN : reason;
  }

  public synchronized void markRandomElimination(long now) {
    lastRandomEliminationAt = now;
    baseTimeMillis = now;
  }

  public synchronized void updateBaseTime(long newBaseTimeMillis) {
    baseTimeMillis = newBaseTimeMillis;
  }

  public synchronized void updateNextAttemptAt(long newNextAttemptAt) {
    nextAttemptAt = newNextAttemptAt;
  }

  public synchronized void resetTransientState() {
    resetConnectionState();
  }

  private void resetConnectionState() {
    dialing = false;
    channelConnected = false;
    handshakeComplete = false;
  }
}