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
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.sun.management.OperatingSystemMXBean;
import io.cdap.cdap.api.Environment;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Represents a service  that runs along with other services to collect resource usage metrics and publish them to
 * {@link MetricsCollectionService}. For this service to work, the jvm process needs to expose JMX server on the same
 * port that this service polls. To do this, the following JAVA OPTS need to be set:
 * {@code -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=11022
 * -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false}
 * This service also requires {@code SERVICE_NAME} and {@code JMX_SERVER_PORT} env variables to be set which provide
 * the component tag for setting metrics context and port to connect to jmx server on localhost respectively.
 */
public class JMXMetricsCollector extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(JMXMetricsCollector.class);
  private static final long MEGA_BYTE = 1024 * 1024, MAX_PORT = (1 << 16) - 1;
  private static final String SERVICE_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";
  private final String componentName;
  private final CConfiguration cConf;
  private final MetricsCollectionService metricsCollectionService;
  private final JMXServiceURL serviceUrl;
  private final Environment env;
  private ScheduledExecutorService executor;

  @Inject
  public JMXMetricsCollector(CConfiguration cConf, MetricsCollectionService metricsCollectionService, Environment env)
    throws MalformedURLException {
    this.cConf = cConf;
    int serverPort = cConf.getInt(Constants.JMXMetricsCollector.SERVER_PORT);
    if (serverPort < 0 || serverPort > MAX_PORT) {
      throw new IllegalArgumentException(
        String.format("%s variable (%d) is not a valid port number.",
                      Constants.JMXMetricsCollector.SERVER_PORT, serverPort));
    }
    String serverUrl = String.format(SERVICE_URL_FORMAT, "localhost", serverPort);
    componentName = env.getVariable(Constants.JMXMetricsCollector.COMPONENT_NAME_ENV_VAR);
    if (componentName == null) {
      throw new IllegalArgumentException(
        "Not collecting resource usage metrics from JMX as SERVICE_NAME env variable is not set.");
    }
    this.metricsCollectionService = metricsCollectionService;
    this.env = env;
    this.serviceUrl = new JMXServiceURL(serverUrl);
  }

  @Override
  protected void startUp() {
    LOG.info(String.format("Starting JMXMetricsCollector."));
  }

  @Override
  protected void shutDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info(String.format("Shutting down JMXMetricsCollector has completed."));
  }

  @Override
  protected void runOneIteration() throws IOException {
    Map<String, String> metricsContext = ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
      Constants.Metrics.Tag.COMPONENT, componentName);
    MetricsCollector metrics = this.metricsCollectionService.getContext(metricsContext);

    try (JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null)) {
      MBeanServerConnection mBeanConn = jmxConnector.getMBeanServerConnection();
      getAndPublishMemoryMetrics(mBeanConn, metrics);
      getAndPublishCPUMetrics(mBeanConn, metrics);
      getAndPublishThreadMetrics(mBeanConn, metrics);
    } catch (Exception e) {
      LOG.error(String.format("Error occurred while connecting to JMX server."), e);
      throw e;
    }
  }

  private void getAndPublishMemoryMetrics(MBeanServerConnection mBeanConn,
                                          MetricsCollector metrics) throws IOException {
    MemoryMXBean mxBean;
    mxBean = ManagementFactory.newPlatformMXBeanProxy(mBeanConn, ManagementFactory.MEMORY_MXBEAN_NAME,
                                                      MemoryMXBean.class);
    MemoryUsage heapMemoryUsage = mxBean.getHeapMemoryUsage();
    metrics.gauge(Constants.Metrics.JVMResource.HEAP_MEMORY_USED_MB,
                  heapMemoryUsage.getUsed() / MEGA_BYTE);
    metrics.gauge(Constants.Metrics.JVMResource.HEAP_MEMORY_MAX_MB,
                  heapMemoryUsage.getMax() / MEGA_BYTE);
  }

  private void getAndPublishCPUMetrics(MBeanServerConnection conn, MetricsCollector metrics) throws IOException {
    OperatingSystemMXBean mxBean;
    mxBean = ManagementFactory.newPlatformMXBeanProxy(conn, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
                                                      OperatingSystemMXBean.class);
    double processCpuLoad = mxBean.getProcessCpuLoad();
    if (processCpuLoad < 0) {
      LOG.info("CPU load for JVM process is not yet available");
      return;
    }
    metrics.gauge(Constants.Metrics.JVMResource.PROCESS_CPU_LOAD_PERCENT,
                  (long) (processCpuLoad * 100));
  }

  private void getAndPublishThreadMetrics(MBeanServerConnection conn, MetricsCollector metrics) throws IOException {
    ThreadMXBean mxBean;
    mxBean = ManagementFactory.newPlatformMXBeanProxy(conn, ManagementFactory.THREAD_MXBEAN_NAME,
                                                      ThreadMXBean.class);
    metrics.gauge(Constants.Metrics.JVMResource.THREAD_COUNT, mxBean.getThreadCount());
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
      0, cConf.getInt(Constants.JMXMetricsCollector.POLL_INTERVAL), TimeUnit.MILLISECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("jmx-metrics-collector"));
    return executor;
  }
}
