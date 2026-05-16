package com.exam.bt.sensor.config;

import com.exam.bt.sensor.service.UdpMessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

@Slf4j
@Component
public class UdpFlowFactory {

  public Connection create(int port, UdpMessageHandler handler) {
    Connection connection =
        UdpServer.create()
            .host("0.0.0.0")
            .port(port)
            .handle((in, out) -> in.receive().asByteArray().flatMap(handler::processMessage).then())
            .bindNow();
    log.info("UDP server started and listening on port {}", port);
    return connection;
  }
}
