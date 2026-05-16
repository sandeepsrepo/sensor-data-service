package com.exam.bt.sensor;

import com.exam.bt.sensor.model.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class SensorDataServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(SensorDataServiceApplication.class, args);
  }
}
