package com.exam.bt.sensor;

import com.exam.bt.sensor.config.UdpFlowFactory;
import com.exam.bt.sensor.model.AppProperties;
import com.exam.bt.sensor.service.UdpMessageHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.netty.Connection;

@Slf4j
@Component
@AllArgsConstructor
public class SensorDataServiceInitialization implements ApplicationRunner, DisposableBean {
  private final AppProperties props;
  private final UdpMessageHandler handler;
  private final UdpFlowFactory flow;

  private final List<Connection> connections = new ArrayList<>();
  private final CountDownLatch latch = new CountDownLatch(1);

  @Override
  public void run(ApplicationArguments args) throws InterruptedException {
    props.udp().ports().forEach(port -> connections.add(flow.create(port, handler)));
    log.info("All UDP servers started. Waiting for messages...");
    latch.await(); // keep the application alive
  }

  @Override
  public void destroy() {
    latch.countDown();
    connections.forEach(Connection::dispose);
    log.info("All UDP connections disposed.");
  }
}
