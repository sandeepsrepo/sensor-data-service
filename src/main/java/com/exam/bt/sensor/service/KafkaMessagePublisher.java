package com.exam.bt.sensor.service;

import com.exam.bt.sensor.model.AppProperties;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Slf4j
@Service
@AllArgsConstructor
public class KafkaMessagePublisher {

  private final KafkaSender<String, String> kafkaSender;
  private final AppProperties appProperties;

  public Mono<Void> publish(String msg) {

    return Mono.fromCallable(
            () -> {
              Map<String, String> sensorData =
                  Arrays.stream(msg.split(";"))
                      .map(String::trim)
                      .map(part -> part.split("=", 2))
                      .filter(arr -> arr.length == 2 && !arr[0].isBlank())
                      .collect(Collectors.toMap(arr -> arr[0].trim(), arr -> arr[1].trim()));
              return SenderRecord.<String, String, String>create(
                  appProperties.kafka().topic(),
                  null,
                  null,
                  sensorData.get("sensor_id"),
                  sensorData.get("value"),
                  null);
            })
        .flatMap(
            record ->
                kafkaSender
                    .send(Mono.just(record))
                    .doOnNext(
                        r ->
                            log.info(
                                "Msg sent to Kafka = key : {}, value : {}, offset : {}, partition : {}",
                                record.key(),
                                record.value(),
                                r.recordMetadata().offset(),
                                r.recordMetadata().partition()))
                    .doOnError(
                        e ->
                            log.error(
                                "Failed to send message to Kafka, key : {}, value : {}",
                                record.key(),
                                record.value(),
                                e))
                    .then());
  }
}
