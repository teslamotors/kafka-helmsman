/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.tesla.data.topic.enforcer.BaseCommand.CommandConfigConverter;

import io.prometheus.client.exporter.HTTPServer;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.Supplier;

public class EnforceCommandTest {

  private String testConf = "---\n" +
      "kafka:\n" +
      "    bootstrap.servers: localhost:9092\n" +
      "\n" +
      "topics:\n" +
      "    - name: topic_a\n" +
      "      partitions: 1\n" +
      "      replicationFactor: 1";

  private CommandConfigConverter converter = new CommandConfigConverter();

  private Enforcer enforcer;
  private HTTPServer metricServer;

  private Map<String, Object> config;

  @Before
  public void setup() throws IOException {
    config = converter.convert(
        new ByteArrayInputStream(testConf.getBytes(Charset.forName("UTF-8"))));
    config.put("metricsPort", findFreePort());
    enforcer = mock(Enforcer.class);
    metricServer = mock(HTTPServer.class);
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  private void check(final EnforceCommand command, final int count) {
    Supplier<Boolean> stop = new Supplier<Boolean>() {
      private int runs = 0;

      @Override
      public Boolean get() {
        return ++runs > count;
      }
    };
    command.doRun(enforcer, stop);
    verify(enforcer, times(count)).stats();
    verify(enforcer, times(count)).enforceAll();
    verifyNoMoreInteractions(enforcer);
  }

  @Test
  public void testNotContinuousMode() {
    EnforceCommand command = new EnforceCommand(config, false, 0);
    check(command, 1);
  }

  @Test
  public void testContinuousModeHappy() {
    EnforceCommand command = new EnforceCommand(config, true, 0);
    check(command, 2);
  }

  @Test
  public void testContinuousModeFailure() {
    // first run fails
    doThrow(new RuntimeException("failed")).doNothing().when(enforcer).enforceAll();
    EnforceCommand command = new EnforceCommand(config, true, 0);
    check(command, 2);
  }

  @Test
  public void testMetricServer() throws IOException {
    EnforceCommand command = spy(new EnforceCommand(config, true, 0));
    doReturn(metricServer).when(command).metricServer(anyInt());
    check(command, 1);
    verify(metricServer).stop();
    verifyNoMoreInteractions(metricServer);
  }

  @Test
  public void testNoMetricServer() throws IOException {
    EnforceCommand command = spy(new EnforceCommand(config, false, 0));
    verify(command, never()).metricServer(anyInt());
  }

  @Test
  public void testAbortIfMetricServerFails() throws IOException {
    EnforceCommand command = spy(new EnforceCommand(config, true, 0));
    doThrow(new IOException("failed")).when(command).metricServer(anyInt());
    check(command, 0);
    verify(command).metricServer(anyInt());
  }

}