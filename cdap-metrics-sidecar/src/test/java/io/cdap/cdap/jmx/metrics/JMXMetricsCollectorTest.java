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

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class JMXMetricsCollectorTest {
  @Test
  public void startStopService() throws  Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    try {
      JMXMetricsCollector jmxMetrics = injector.getInstance(JMXMetricsCollector.class);
      System.out.println(jmxMetrics);
      Service.State state = jmxMetrics.startAndWait();
      Assert.assertSame(state, Service.State.RUNNING);

      state = jmxMetrics.stopAndWait();
      Assert.assertSame(state, Service.State.TERMINATED);
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }

  @Test
  public void startStopServer() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    try {
      AppFabricServer server = injector.getInstance(AppFabricServer.class);
      DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
      Service.State state = server.startAndWait();
      Assert.assertSame(state, Service.State.RUNNING);

      final EndpointStrategy endpointStrategy = new RandomEndpointStrategy(
        () -> discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP));
      Assert.assertNotNull(endpointStrategy.pick(5, TimeUnit.SECONDS));

      state = server.stopAndWait();
      Assert.assertSame(state, Service.State.TERMINATED);

      Tasks.waitFor(true, () -> endpointStrategy.pick() == null, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      AppFabricTestHelper.shutdown();
    }
  }
}