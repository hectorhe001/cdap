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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXMetricsCollector extends AbstractScheduledService {
  private final CConfiguration cConf;
  private static final Logger LOG = LoggerFactory.getLogger(JMXMetricsCollector.class);
  private final String serverUrl;
  private static final long megaByte = 1024 * 1024;
  public final String podName;
  private ScheduledExecutorService executor;
  private MetricsCollectionService metricsCollectionService;

  @Inject
  public JMXMetricsCollector(CConfiguration cConf, @Nullable MetricsCollectionService metricsCollectionService) {
    this.cConf = cConf;
    this.serverUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", "localhost",
                                   cConf.getInt(Constants.JMXMetricsCollector.SERVER_PORT));
    this.podName = System.getenv("HOSTNAME");
    try {
      this.metricsCollectionService = metricsCollectionService;
    } catch (Exception e) {
      LOG.error(String.format("@arjansbal: %s", e.getMessage()));
    }
  }

  @Override
  protected void startUp() {
    LOG.info(String.format("Starting JMXMetricsCollector in pod %s.", this.podName));
  }

  @Override
  protected void shutDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info(String.format("Shutting down JMXMetricsCollector in pod %s has completed.", this.podName));
  }

  @Override
  protected void runOneIteration() {
    MBeanServerConnection mBeanConn;
    MetricsCollector metrics;
    try {
      Map<String, String> metricsContext = ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace(),
        Constants.Metrics.Tag.COMPONENT, this.podName);
      metrics = this.metricsCollectionService.getContext(metricsContext);
      JMXServiceURL serviceUrl = new JMXServiceURL(serverUrl);
      JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
      mBeanConn = jmxConnector.getMBeanServerConnection();
    } catch (Exception e) {
      LOG.warn(String.format("Error occurred while connecting to JMX server in pod %s: %s",
                             this.podName,  e.getMessage()));
      return;
    }
    getAndPublishMemoryMetrics(mBeanConn, metrics);
    getAndPublishCPUMetrics(mBeanConn, metrics);
    getAndPublishThreadMetrics(mBeanConn, metrics);
  }

  private void getAndPublishMemoryMetrics(MBeanServerConnection mBeanConn, MetricsCollector metrics) {
    MemoryMXBean mxBean;
    try {
      mxBean = ManagementFactory.newPlatformMXBeanProxy(mBeanConn, ManagementFactory.MEMORY_MXBEAN_NAME,
                                MemoryMXBean.class);
    } catch (IOException e) {
      LOG.warn("Error occurred while collecting memory metrics from JMX: " + e.getMessage());
      return;
    }
    MemoryUsage heapMemoryUsage = mxBean.getHeapMemoryUsage();
    metrics.gauge(Constants.Metrics.JVMResource.HEAP_MEMORY_USED_MB,
                                heapMemoryUsage.getUsed() / megaByte);
    metrics.gauge(Constants.Metrics.JVMResource.HEAP_MEMORY_MAX_MB,
                                heapMemoryUsage.getMax() / megaByte);
  }

  private void getAndPublishCPUMetrics(MBeanServerConnection conn, MetricsCollector metrics) {
    OperatingSystemMXBean mxBean;
    try {
      mxBean = ManagementFactory.newPlatformMXBeanProxy(conn, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
                                OperatingSystemMXBean.class);
    } catch (IOException e) {
      LOG.warn("Error occurred while collecting memory metrics from JMX: " + e.getMessage());
      return;
    }
    double systemCPULoad = mxBean.getSystemCpuLoad();
    if (systemCPULoad < 0) {
      LOG.info("CPU load fro JVM process is not yet available");
      return;
    }
    metrics.gauge(Constants.Metrics.JVMResource.PROCESS_CPU_LOAD_PERCENT,
                                (long) systemCPULoad * 100);
  }

  private void getAndPublishThreadMetrics(MBeanServerConnection conn, MetricsCollector metrics) {
    ThreadMXBean mxBean;
    try {
      mxBean = ManagementFactory.newPlatformMXBeanProxy(conn, ManagementFactory.THREAD_MXBEAN_NAME,
                                ThreadMXBean.class);
    } catch (IOException e) {
      LOG.warn("Error occurred while collecting thread metrics from JMX: " + e.getMessage());
      return;
    }
    metrics.gauge(Constants.Metrics.JVMResource.THREAD_COUNT, mxBean.getThreadCount());
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0,
                                          cConf.getInt(Constants.JMXMetricsCollector.POLL_INTERVAL), TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("jmx-metrics-collector"));
    return executor;
  }
}
