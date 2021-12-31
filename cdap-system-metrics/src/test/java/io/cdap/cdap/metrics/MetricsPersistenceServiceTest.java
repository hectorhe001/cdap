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

package io.cdap.cdap.metrics;

import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.common.utils.Tasks;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

public class MetricsPersistenceServiceTest {

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
    metricsBuffer.addAll(getMockMetricArray(bufferCapacity));
    MetricsPersistenceService service = new MetricsPersistenceService(metricsBuffer, publisher, 1);
    service.start();
    Assert.assertEquals(bufferCapacity, metricsBuffer.size());
    Tasks.waitFor(0, () -> metricsBuffer.size(),2, TimeUnit.SECONDS);
    service.stop();
    verify(publisher, atLeast(1)).publish(any());
  }

  private Collection<MetricValues> getMockMetricArray(int length) {
    MetricValue metric = new MetricValue("mock.metric", MetricType.COUNTER, 1);
    long now = System.currentTimeMillis();
    MetricValues metricValues = new MetricValues(new TreeMap<>(),
                                                 metric.getName(),
                                                 TimeUnit.MILLISECONDS.toSeconds(now),
                                                 metric.getValue(), metric.getType());
    return new ArrayList<>(Collections.nCopies(length, metricValues));
  }
}