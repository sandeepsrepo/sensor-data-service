package com.exam.bt.sensor.config;

import com.exam.bt.sensor.service.UdpMessageHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.handler.ReactiveMessageHandlerAdapter;
import org.springframework.integration.ip.udp.UnicastReceivingChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class UdpFlowFactory {

  public IntegrationFlow create(int port, UdpMessageHandler handler) {

    UnicastReceivingChannelAdapter adapter = new UnicastReceivingChannelAdapter(port);

    return IntegrationFlow.from(adapter)
        .channel(c -> c.flux())
        .handle(
            new ReactiveMessageHandlerAdapter(msg -> handler.processMessage((Message<byte[]>) msg)))
        .get();
  }
}
