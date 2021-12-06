/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.jmx.metrics;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.Environment;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JMXMetricsCollectorTest {
  private static final int serverPort = 11023;
  private static final String serviceName = "test-service";
  private static JMXConnectorServer svr;

  private final Map<String, String> metricsContext = ImmutableMap.of(
    Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
    Constants.Metrics.Tag.COMPONENT, serviceName);

  @Mock
  private MetricsCollectionService mockMetricsService;
  @Mock
  private MetricsContext mockContext;
  @Mock
  private Environment mockEnv;

  @Before
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
    when(mockMetricsService.getContext(metricsContext)).thenReturn(mockContext);
    when(mockEnv.getVariable("HOSTNAME")).thenReturn("test-host");
  }

  @BeforeClass
  public static void setupClass() throws IOException {
    svr = createJMXConnectorServer(serverPort);
    svr.start();
    Assert.assertEquals(svr.isActive(), true);
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    svr.stop();
  }

  @Test
  public void testInvalidPortInConfig() throws InterruptedException, MalformedURLException {
    when(mockEnv.getVariable("SERVICE_NAME")).thenReturn(serviceName);
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL, 100);
    cConf.setInt(Constants.JMXMetricsCollector.SERVER_PORT, -1);
    JMXMetricsCollector jmxMetrics = new JMXMetricsCollector(cConf, mockMetricsService, mockEnv);
    jmxMetrics.start();
    Thread.sleep(200);
    jmxMetrics.stop();
    verify(mockContext, never()).gauge(anyString(), anyLong());
  }

  @Test
  public void testMissingServiceName() throws InterruptedException, MalformedURLException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL, 100);
    cConf.setInt(Constants.JMXMetricsCollector.SERVER_PORT, serverPort);
    JMXMetricsCollector jmxMetrics = new JMXMetricsCollector(cConf, mockMetricsService, mockEnv);
    jmxMetrics.start();
    Thread.sleep(200);
    jmxMetrics.stop();
    verify(mockContext, never()).gauge(anyString(), anyLong());
  }

  @Test
  public void testNumberOfMetricsEmitted() throws InterruptedException, MalformedURLException {
    when(mockEnv.getVariable("SERVICE_NAME")).thenReturn(serviceName);
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL, 100);
    cConf.setInt(Constants.JMXMetricsCollector.SERVER_PORT, serverPort);
    JMXMetricsCollector jmxMetrics = new JMXMetricsCollector(cConf, mockMetricsService, mockEnv);
    jmxMetrics.start();
    // Poll should run at 0, 100, 200 & 300 millis and 20 millis buffer.
    Thread.sleep(320);
    jmxMetrics.stop();
    verify(mockContext, times(4)).gauge(eq(Constants.Metrics.JVMResource.THREAD_COUNT), anyLong());
    verify(mockContext, times(4)).gauge(eq(Constants.Metrics.JVMResource.HEAP_MEMORY_MAX_MB), anyLong());
    verify(mockContext, times(4)).gauge(eq(Constants.Metrics.JVMResource.HEAP_MEMORY_USED_MB), anyLong());
    verify(mockContext, times(4)).gauge(eq(Constants.Metrics.JVMResource.PROCESS_CPU_LOAD_PERCENT), anyLong());
  }

  private static JMXConnectorServer createJMXConnectorServer(int port) throws IOException {
    LocateRegistry.createRegistry(port);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    JMXServiceURL url = new JMXServiceURL(
      String.format("service:jmx:rmi://localhost/jndi/rmi://localhost:%d/jmxrmi", port));
    return JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
  }
}
