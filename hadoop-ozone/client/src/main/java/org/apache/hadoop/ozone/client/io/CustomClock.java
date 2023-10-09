package org.apache.hadoop.ozone.client.io;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;

public class CustomClock extends Clock{


  private Instant instant;
  private final ZoneId zoneId;

  public static CustomClock newInstance() {
    return new CustomClock(Instant.now(), ZoneOffset.UTC);
  }

  public CustomClock(Instant instant, ZoneId zone) {
    this.instant = instant;
    this.zoneId = zone;
  }

  @Override
  public ZoneId getZone() {
    return zoneId;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new CustomClock(Instant.now(), zone);
  }

  @Override
  public Instant instant() {
    return instant;
  }

  public void fastForward(long millis) {
    set(instant().plusMillis(millis));
  }

  public void fastForward(TemporalAmount temporalAmount) {
    set(instant().plus(temporalAmount));
  }

  public void rewind(long millis) {
    set(instant().minusMillis(millis));
  }

  public void rewind(TemporalAmount temporalAmount) {
    set(instant().minus(temporalAmount));
  }

  public void set(Instant newInstant) {
    this.instant = newInstant;
  }
}
