package com.exam.bt.sensor.model;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "sensors")
public record AppProperties(Udp udp, Kafka kafka) {
  public record Udp(List<Integer> ports) {}

  public record Kafka(String topic, String bootstrapServers) {}
}
