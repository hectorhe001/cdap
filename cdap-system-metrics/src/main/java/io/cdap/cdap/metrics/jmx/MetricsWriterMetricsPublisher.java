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

package io.cdap.cdap.metrics.jmx;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.metrics.process.DefaultMetricsWriterContext;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MetricsWriterMetricsPublisher implements MetricsPublisher {
  private Map<String, String> metricTags;
  private final Map<String, MetricsWriter> metricsWriters;
  private boolean initialized = false;
  private final int persistingFrequencySeconds;
  private final int maxMetricsCapacity;
  private final BlockingQueue<MetricValues> metricValues;
  private final CConfiguration cConf;
  private static final Logger LOG = LoggerFactory.getLogger(MetricsWriterMetricsPublisher.class);

  @Inject
  public MetricsWriterMetricsPublisher(CConfiguration cConf, MetricsWriterProvider writerProvider) {
    this.metricsWriters = writerProvider.loadMetricsWriters();
    this.cConf = cConf;
    // TODO: define vars
    this.persistingFrequencySeconds = cConf.getInt("abc");
    this.maxMetricsCapacity = cConf.getInt("abc");
    this.metricValues = new ArrayBlockingQueue<>(this.maxMetricsCapacity);
  }

  @Override
  public void initialize(Map<String, String> metricsContext) {
    if (this.initialized) {
      throw new IllegalStateException("Can't initialize publisher multiple times");
    }
    this.metricTags = metricsContext;
    this.initialized = true;

    initializeMetricWriters(this.metricsWriters);
    // start thread to
  }

  @VisibleForTesting
  private void initializeMetricWriters(Map<String, MetricsWriter> metricsWriters) {
    for (Map.Entry<String, MetricsWriter> entry : metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      // Metrics context used by MetricsStoreMetricsWriter only, which we don't use here
      // So we can pass noop context
      DefaultMetricsWriterContext metricsWriterContext =
        new DefaultMetricsWriterContext(new NoopMetricsContext, this.cConf, writer.getID());
      writer.initialize(metricsWriterContext);
    }
  }

  @Override
  public void publish(Collection<MetricValue> metrics) {
    if (this.metricTags == null) {
      throw new IllegalStateException("Initialize publisher by calling initialize() before publishing metrics");
    }
    for (MetricValue metric : metrics) {
      boolean inserted = this.metricValues.offer(
        new MetricValues(this.metricTags, metric.getName(),
                         new Date().getTime(), metric.getValue(), metric.getType()));
      if (!inserted) {
        throw new IllegalStateException("Discarding metrics since queue is full");
      }
    }
  }

  @Override
  public void stop() {

  }

  @Override
  protected void run() throws Exception {

  }
}
