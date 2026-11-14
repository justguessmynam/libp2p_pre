package org.tron.p2p.connection.business.random;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.tron.p2p.discover.Node;

public class TargetStateRepo {

  // Target address to its current scheduling and connection state.
  private final Map<InetSocketAddress, TargetPeerState> stateMap = new ConcurrentHashMap<>();

  public void put(TargetPeerState state) {
    if (state == null) {
      throw new IllegalArgumentException("state is null");
    }
    stateMap.put(state.getAddress(), state);
  }

  public boolean putIfAbsent(TargetPeerState state) {
    if (state == null) {
      throw new IllegalArgumentException("state is null");
    }
    return stateMap.putIfAbsent(state.getAddress(), state) == null;
  }

  public TargetPeerState get(InetSocketAddress address) {
    if (address == null) {
      return null;
    }
    return stateMap.get(address);
  }

  public TargetPeerState get(Node node) {
    InetSocketAddress address = getAddress(node);
    return address == null ? null : stateMap.get(address);
  }

  public boolean contains(InetSocketAddress address) {
    return address != null && stateMap.containsKey(address);
  }

  public boolean contains(Node node) {
    InetSocketAddress address = getAddress(node);
    return address != null && stateMap.containsKey(address);
  }

  public TargetPeerState getOrCreate(Node node, long baseTimeMillis) {
    InetSocketAddress address = requireAddress(node);
    return stateMap.computeIfAbsent(
        address, ignored -> new TargetPeerState(node, baseTimeMillis));
  }

  public TargetPeerState remove(InetSocketAddress address) {
    if (address == null) {
      return null;
    }
    return stateMap.remove(address);
  }

  public TargetPeerState remove(Node node) {
    InetSocketAddress address = getAddress(node);
    return address == null ? null : stateMap.remove(address);
  }

  public Collection<TargetPeerState> all() {
    return new ArrayList<>(stateMap.values());
  }

  public Collection<TargetPeerState> allReadOnly() {
    return Collections.unmodifiableCollection(
        new ArrayList<>(stateMap.values()));
  }

  public int size() {
    return stateMap.size();
  }

  public boolean isEmpty() {
    return stateMap.isEmpty();
  }

  public void clear() {
    stateMap.clear();
  }

  private static InetSocketAddress getAddress(Node node) {
    return node == null ? null : node.getPreferInetSocketAddress();
  }

  private static InetSocketAddress requireAddress(Node node) {
    InetSocketAddress address = getAddress(node);
    if (address == null) {
      throw new IllegalArgumentException("node or node address is null");
    }
    return address;
  }
}