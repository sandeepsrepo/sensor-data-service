package com.exam.bt.sensor;

import com.exam.bt.sensor.config.UdpFlowFactory;
import com.exam.bt.sensor.model.AppProperties;
import com.exam.bt.sensor.service.UdpMessageHandler;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class SensorDataServiceInitialization implements ApplicationRunner {
  private final AppProperties props;
  private final UdpMessageHandler handler;
  private final UdpFlowFactory flow;
  private final IntegrationFlowContext context;

  @Override
  public void run(ApplicationArguments args) {
    props
        .udp()
        .ports()
        .forEach(
            port ->
                context
                    .registration(flow.create(port, handler))
                    .id("udp-" + port)
                    .register()
                    .start());
  }
}
