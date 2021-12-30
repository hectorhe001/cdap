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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.metrics.process.DefaultMetricsWriterContext;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class MetricsWriterMetricsPublisher extends AbstractMetricsPublisher {
  private final Map<String, MetricsWriter> metricsWriters;
  private static final Logger LOG = LoggerFactory.getLogger(MetricsWriterMetricsPublisher.class);

  @Inject
  public MetricsWriterMetricsPublisher(MetricsWriterProvider writerProvider, CConfiguration cConf) {
    this.metricsWriters = writerProvider.loadMetricsWriters();
    initializeMetricWriters(this.metricsWriters, cConf);
  }

  @VisibleForTesting
  private static void initializeMetricWriters(Map<String, MetricsWriter> metricsWriters, CConfiguration cConf) {
    for (Map.Entry<String, MetricsWriter> entry : metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      // Metrics context used by MetricsStoreMetricsWriter only, which we don't use here
      // So we can pass noop context
      DefaultMetricsWriterContext metricsWriterContext =
        new DefaultMetricsWriterContext(new NoopMetricsContext(), cConf, writer.getID());
      writer.initialize(metricsWriterContext);
    }
  }

  @Override
  public void publish(Collection<MetricValues> metrics) throws Exception {
    if (metrics.isEmpty()) {
      return;
    }
    Exception exceptionCollector = null;
    for (Map.Entry<String, MetricsWriter> entry : this.metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      try {
        writer.write(metrics);
      } catch (Exception e) {
        if (exceptionCollector == null) {
          exceptionCollector = e;
        } else {
          exceptionCollector.addSuppressed(e);
        }
        continue;
      }
      LOG.info("{} metrics persisted using {} metrics writer.", metrics.size(), writer.getID());
    }
    if (exceptionCollector != null) {
      throw exceptionCollector;
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, MetricsWriter> entry : metricsWriters.entrySet()) {
      MetricsWriter writer = entry.getValue();
      writer.close();
    }
  }
}
