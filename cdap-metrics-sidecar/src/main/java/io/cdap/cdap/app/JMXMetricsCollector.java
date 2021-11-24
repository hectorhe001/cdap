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

package io.cdap.cdap.app;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXMetricsCollector extends AbstractScheduledService {
  private final CConfiguration cConf;
  private final Configuration hConf;
  private static final Logger LOG = LoggerFactory.getLogger(JMXMetricsCollector.class);

  @Inject
  public JMXMetricsCollector(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting JMXMetricsCollector.");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down JMXMetricsCollector has completed.");
  }

  private ResourceUsageMetrics getServiceUsageMetrics() {
    // TODO: Fetch port from cconf
    String url = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", "localhost", 11022);
    MemoryMXBean memoryMXBean = null;

    try {
      JMXServiceURL serviceUrl = new JMXServiceURL(url);
      JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
      MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();

      memoryMXBean = ManagementFactory
        .newPlatformMXBeanProxy(mbeanConn, ManagementFactory.MEMORY_MXBEAN_NAME,
                                MemoryMXBean.class);
      // TODO: Add MXBeans for CPU and Threads
    } catch (IOException e) {
      // TODO: Get pod name from variable
      LOG.warn("Error occurred while collecting JMX metrics from Appfabric pods: " + e.getMessage());
    }
    MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
    return new ResourceUsageMetrics(heapMemoryUsage.getUsed(), heapMemoryUsage.getMax());
  }

  private  void publishServiceUsageMetrics(ResourceUsageMetrics metrics) {
    // TODO: Implement this
  }

  @Override
  protected void runOneIteration() throws Exception {
    ResourceUsageMetrics metrics = getServiceUsageMetrics();
    // Code to publish these metrics
  }

  @Override
  protected Scheduler scheduler() {
    // TODO: Replace time period with cConf.getInt(Constants.JMXMetricsCollector.POLL_INTERVAL)
    return Scheduler.newFixedRateSchedule(0,
                                          10, TimeUnit.SECONDS);
  }
}
