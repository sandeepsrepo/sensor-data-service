package com.exam.bt.sensor.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@AllArgsConstructor
public class UdpMessageHandler {

  private final KafkaMessagePublisher messagePublisher;

  public Mono<Void> processMessage(Message<byte[]> message) {
    return Mono.fromCallable(() -> new String(message.getPayload()))
        .filter(payload -> !payload.isBlank())
        .switchIfEmpty(Mono.fromRunnable(() -> log.info("Empty message received from UDP")))
        .flatMap(messagePublisher::publish)
        .then();
  }
}
