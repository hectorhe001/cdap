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

package io.cdap.cdap.metrics.publisher;

import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.common.utils.Tasks;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MetricsPersistenceServiceTest extends AbstractPublisherTest {

  @Mock
  private MetricsPublisher publisher;

  @Before
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testMetricsDrain() throws Exception {
    int bufferCapacity = 8;
    BlockingQueue<MetricValues> metricsBuffer = new ArrayBlockingQueue<>(bufferCapacity);
    Collection<MetricValues> metricValues = getMockMetricValuesArray(bufferCapacity);
    metricsBuffer.addAll(metricValues);
    MetricsPersistenceService service = new MetricsPersistenceService(metricsBuffer, publisher, 1);
    service.start();
    Assert.assertEquals(bufferCapacity, metricsBuffer.size());
    Tasks.waitFor(0, () -> metricsBuffer.size(), 2, TimeUnit.SECONDS);
    service.stop();
    // Check publisher is called and with metrics in correct order
    verify(publisher, times(1)).publish(metricValues);
  }
}