package com.exam.bt.sensor;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {"sensors.udp.ports=19344", "sensors.kafka.topic=test-sensor-topic"})
@Testcontainers
class SensorDataIntegrationTest {

  private static final int TEST_UDP_PORT = 19344;
  private static final String TEST_TOPIC = "test-sensor-topic";

  @Container
  static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

  @DynamicPropertySource
  static void kafkaProperties(DynamicPropertyRegistry registry) {
    registry.add("sensors.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  private KafkaConsumer<String, String> consumer;

  @BeforeEach
  void setUp() {
    consumer =
        new KafkaConsumer<>(
            Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis(),
                AUTO_OFFSET_RESET_CONFIG, "earliest",
                KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
    consumer.subscribe(List.of(TEST_TOPIC));
  }

  @AfterEach
  void tearDown() {
    consumer.close();
  }

  @Test
  void shouldForwardUdpMessageToKafka() throws Exception {
    byte[] data = "sensor_id=t1; value=31".getBytes();
    try (DatagramSocket socket = new DatagramSocket()) {
      DatagramPacket packet =
          new DatagramPacket(data, data.length, InetAddress.getLoopbackAddress(), TEST_UDP_PORT);
      socket.send(packet);
    }

    ConsumerRecords<String, String> records = ConsumerRecords.empty();
    long deadline = System.currentTimeMillis() + 10_000;
    while (records.isEmpty() && System.currentTimeMillis() < deadline) {
      records = consumer.poll(Duration.ofMillis(500));
    }

    assertThat(records.count()).isEqualTo(1);
    ConsumerRecord<String, String> record = records.iterator().next();
    assertThat(record.key()).isEqualTo("t1");
    assertThat(record.value()).isEqualTo("31");
  }
}
