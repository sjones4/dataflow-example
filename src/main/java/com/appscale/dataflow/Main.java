/*
 * Copyright 2018 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.dataflow;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.Bean;

/**
 *
 */
@EnableAutoConfiguration
public class Main {

  @Bean
  public DataflowController dataflowController() {
    return new DataflowController();
  }

  @Bean
  public GoogGenericJsonHttpMessageConverter googMessageConverter() {
    return new GoogGenericJsonHttpMessageConverter();
  }

  public static void main(final String[] args) {
    SpringApplication.run(Main.class, args);
  }

}
