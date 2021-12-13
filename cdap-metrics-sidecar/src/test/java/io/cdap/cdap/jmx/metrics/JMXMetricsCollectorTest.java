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
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.exceptions.verification.TooLittleActualInvocations;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JMXMetricsCollectorTest {
  private static final int SERVER_PORT = 11023;
  private static final String SERVICE_NAME = "test-service";
  private static JMXConnectorServer svr;

  private static final Map<String, String> METRICS_CONTEXT = ImmutableMap.of(
    Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
    Constants.Metrics.Tag.COMPONENT, SERVICE_NAME);

  @Mock
  private MetricsCollectionService mockMetricsService;
  @Mock
  private MetricsContext mockContext;
  @Mock
  private Environment mockEnv;

  @Before
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
    when(mockMetricsService.getContext(METRICS_CONTEXT)).thenReturn(mockContext);
  }

  @BeforeClass
  public static void setupClass() throws IOException {
    svr = createJMXConnectorServer(SERVER_PORT);
    svr.start();
    Assert.assertEquals(svr.isActive(), true);
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    svr.stop();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPortInConfig() throws Exception {
    when(mockEnv.getVariable(Constants.JMXMetricsCollector.COMPONENT_NAME_ENV_VAR)).thenReturn(SERVICE_NAME);
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JMXMetricsCollector.JMX_SERVER_PORT, -1);
    cConf.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL, 100);
    JMXMetricsCollector jmxMetrics = new JMXMetricsCollector(cConf, mockMetricsService, mockEnv);
    jmxMetrics.start();
    jmxMetrics.stop();
    throw new Exception("Service should not have started successfully with Invalid Port");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingServiceName() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JMXMetricsCollector.JMX_SERVER_PORT, SERVER_PORT);
    cConf.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL, 100);
    JMXMetricsCollector jmxMetrics = new JMXMetricsCollector(cConf, mockMetricsService, mockEnv);
    jmxMetrics.start();
    jmxMetrics.stop();
    throw new Exception("Service should not have started successfully with missing service name");
  }

  @Test
  public void testNumberOfMetricsEmitted() throws InterruptedException, MalformedURLException,
    ExecutionException, TimeoutException {
    when(mockEnv.getVariable(Constants.JMXMetricsCollector.COMPONENT_NAME_ENV_VAR)).thenReturn(SERVICE_NAME);
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JMXMetricsCollector.JMX_SERVER_PORT, SERVER_PORT);
    cConf.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL, 100);
    JMXMetricsCollector jmxMetrics = new JMXMetricsCollector(cConf, mockMetricsService, mockEnv);
    jmxMetrics.start();
    // Poll should run at 0, 100. 500 millis buffer.
    Tasks.waitFor(true, () -> {
      try {
        verify(mockContext, atLeast(2)).gauge(eq(Constants.Metrics.JVMResource.THREAD_COUNT), anyLong());
        verify(mockContext, atLeast(2)).gauge(eq(Constants.Metrics.JVMResource.HEAP_MEMORY_MAX_MB), anyLong());
        verify(mockContext, atLeast(2)).gauge(eq(Constants.Metrics.JVMResource.HEAP_MEMORY_USED_MB), anyLong());
        verify(mockContext, atLeast(2)).gauge(eq(Constants.Metrics.JVMResource.PROCESS_CPU_LOAD_PERCENT), anyLong());
      } catch (TooLittleActualInvocations e) {
        return false;
      }
      return true;
    }, 600, TimeUnit.MILLISECONDS);

    jmxMetrics.stop();
  }

  private static JMXConnectorServer createJMXConnectorServer(int port) throws IOException {
    LocateRegistry.createRegistry(port);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    JMXServiceURL url = new JMXServiceURL(
      String.format("service:jmx:rmi://localhost/jndi/rmi://localhost:%d/jmxrmi", port));
    return JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
  }
}
