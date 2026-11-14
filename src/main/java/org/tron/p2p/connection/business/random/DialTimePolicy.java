package org.tron.p2p.connection.business.random;

import lombok.Getter;

@Getter
public class DialTimePolicy {

  // Interval between two adjacent connection time slots.
  private final long gridMillis;

  // Minimum interval between attempts to the same target.
  private final long minAttemptIntervalMillis;

  // Minimum time required before an upcoming connection slot.
  private final long minLeadMillis;

  public DialTimePolicy(
      long gridMillis,
      long minAttemptIntervalMillis,
      long minLeadMillis) {
    if (gridMillis <= 0) {
      throw new IllegalArgumentException("gridMillis must be greater than 0");
    }
    if (minAttemptIntervalMillis <= 0) {
      throw new IllegalArgumentException(
          "minAttemptIntervalMillis must be greater than 0");
    }
    if (minLeadMillis < 0) {
      throw new IllegalArgumentException(
          "minLeadMillis must not be negative");
    }

    this.gridMillis = gridMillis;
    this.minAttemptIntervalMillis = minAttemptIntervalMillis;
    this.minLeadMillis = minLeadMillis;
  }

  public long nextAttemptTime(
      long baseTimeMillis,
      long now,
      long cooldownUntil,
      long lastAttemptAt) {
    long earliest = now + minLeadMillis;

    if (cooldownUntil > earliest) {
      earliest = cooldownUntil;
    }

    if (lastAttemptAt > 0) {
      long earliestByAttemptInterval = lastAttemptAt + minAttemptIntervalMillis;
      if (earliestByAttemptInterval > earliest) {
        earliest = earliestByAttemptInterval;
      }
    }

    return ceilToGrid(baseTimeMillis, earliest);
  }

  public long ceilToGrid(long baseTimeMillis, long referenceTime) {
    if (referenceTime <= baseTimeMillis) {
      return baseTimeMillis;
    }

    long delta = referenceTime - baseTimeMillis;
    long remainder = delta % gridMillis;

    if (remainder == 0) {
      return referenceTime;
    }

    return referenceTime + gridMillis - remainder;
  }

  public long gridIndex(long baseTimeMillis, long timeMillis) {
    if (timeMillis < baseTimeMillis) {
      return -1L;
    }

    return (timeMillis - baseTimeMillis) / gridMillis;
  }

  public boolean isExactGridTime(long baseTimeMillis, long timeMillis) {
    if (timeMillis < baseTimeMillis) {
      return false;
    }

    return (timeMillis - baseTimeMillis) % gridMillis == 0;
  }
}