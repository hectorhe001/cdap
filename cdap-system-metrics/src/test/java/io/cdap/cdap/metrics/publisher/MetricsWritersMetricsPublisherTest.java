/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetricsWritersMetricsPublisherTest extends AbstractPublisherTest {
  @Test
  public void testPublishFlow() throws Exception {
    int numWriters = 5;
    Map<String, MetricsWriter> writers = getMockWriters(numWriters);
    MetricsWriterProvider provider = mock(MetricsWriterProvider.class);
    when(provider.loadMetricsWriters()).thenReturn(writers);
    CConfiguration cConf = CConfiguration.create();
    MetricsWritersMetricsPublisher publisher = new MetricsWritersMetricsPublisher(provider, cConf);
    // Ensure all writers are initialized
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(1)).initialize(any());
      verify(writer, times(0)).write(any());
    }
    Collection<MetricValues> metricValues = getMockMetricValuesArray(5);
    publisher.publish(metricValues);
    // Ensure all writers receive the same metrics
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(1)).write(metricValues);
    }
    // Ensure if one writer throws exception on write(), rest still get metrics
    MetricsWriter faultyWriter = writers.get(getWriterName(2));
    doThrow(IOException.class).when(faultyWriter).write(metricValues);
    try {
      publisher.publish(metricValues);
      Assert.fail("Exception thrown by writer not propagated upwards from publisher.publish()");
    } catch (IOException e) {
      // no-op
    }
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(2)).write(metricValues);
    }
    publisher.close();
    // Ensure all writers are closed
    for (Map.Entry<String, MetricsWriter> entry : writers.entrySet()) {
      MetricsWriter writer = entry.getValue();
      verify(writer, times(1)).close();
    }
  }

  private Map<String, MetricsWriter> getMockWriters(int count) {
    Map<String, MetricsWriter> writers = new TreeMap<>();
    for (int i = 0; i < count; ++i) {
      MetricsWriter writer = mock(MetricsWriter.class);
      String name = getWriterName(i);
      when(writer.getID()).thenReturn(name);
      writers.put(name, writer);
    }
    return writers;
  }

  private String getWriterName(int index) {
    return String.format("Writer #%d", index);
  }
}
