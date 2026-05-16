package com.exam.bt.sensor.config;

import com.exam.bt.sensor.model.AppProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

@Configuration
@AllArgsConstructor
public class KafkaConfig {

  private final AppProperties appProperties;

  @Bean
  public SenderOptions<String, String> senderOptions() {
    Map props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.kafka().bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return SenderOptions.<String, String>create(props);
  }

  @Bean
  public KafkaSender kafkaSender() {
    return KafkaSender.create(senderOptions());
  }
}
