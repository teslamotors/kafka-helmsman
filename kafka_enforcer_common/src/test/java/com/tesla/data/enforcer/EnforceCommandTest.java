/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.tesla.data.enforcer.BaseCommand.CommandConfigConverter;

import io.prometheus.client.exporter.HTTPServer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Supplier;

public class EnforceCommandTest {

  private static final String testConf = "---\n" +
      "kafka:\n" +
      "    bootstrap.servers: localhost:9092\n" +
      "\n" +
      "dummies:\n" +
      "    - name: a\n" +
      "      config: {}\n";
  private final CommandConfigConverter converter = new CommandConfigConverter();

  @Mock
  private Enforcer<Dummy> enforcer;
  private HTTPServer metricServer;

  private Map<String, Object> config;

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  @Before
  public void setup() throws IOException {
    MockitoAnnotations.initMocks(this);
    config = converter.convert(
        new ByteArrayInputStream(testConf.getBytes(StandardCharsets.UTF_8)));
    config.put("metricsPort", findFreePort());
    metricServer = mock(HTTPServer.class);
  }

  private void check(final EnforceCommand<Dummy> command, final int count) {
    Supplier<Boolean> stop = new Supplier<Boolean>() {
      private int runs = 0;

      @Override
      public Boolean get() {
        return ++runs > count;
      }
    };
    command.doRun(stop);
    verify(enforcer, times(count)).stats();
    verify(enforcer, times(count)).enforceAll();
    verifyNoMoreInteractions(enforcer);
  }

  @Test
  public void testNotContinuousMode() {
    EnforceCommand<Dummy> command = new EnforceCommand<>(enforcer, config, false, 0);
    check(command, 1);
  }

  @Test
  public void testContinuousModeHappy() {
    EnforceCommand<Dummy> command = new EnforceCommand<>(enforcer, config, true, 0);
    check(command, 2);
  }

  @Test
  public void testContinuousModeFailure() {
    // first run fails
    doThrow(new RuntimeException("failed")).doNothing().when(enforcer).enforceAll();
    EnforceCommand<Dummy> command = new EnforceCommand<>(enforcer, config, true, 0);
    check(command, 2);
  }

  @Test
  public void testMetricServer() throws IOException {
    EnforceCommand<Dummy> command = spy(new EnforceCommand<>(enforcer, config, true, 0));
    doReturn(metricServer).when(command).metricServer(anyInt());
    check(command, 1);
    verify(metricServer).stop();
    verifyNoMoreInteractions(metricServer);
  }

  @Test
  public void testNoMetricServer() throws IOException {
    EnforceCommand<Dummy> command = spy(new EnforceCommand<>(enforcer, config, false, 0));
    verify(command, never()).metricServer(anyInt());
  }

  @Test
  public void testAbortIfMetricServerFails() throws IOException {
    EnforceCommand<Dummy> command = spy(new EnforceCommand<>(enforcer, config, true, 0));
    doThrow(new IOException("failed")).when(command).metricServer(anyInt());
    check(command, 0);
    verify(command).metricServer(anyInt());
  }

}