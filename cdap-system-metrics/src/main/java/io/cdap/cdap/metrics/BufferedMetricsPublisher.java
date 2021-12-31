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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Class that uses the Decorator pattern to wrap another {@link MetricsPublisher}.
 * This class limits the rate at which metrics are published to the wrapped publisher.
 */
public class BufferedMetricsPublisher extends AbstractMetricsPublisher {
  private final BlockingQueue<MetricValues> metricValues;
  private static final Logger LOG = LoggerFactory.getLogger(BufferedMetricsPublisher.class);
  public static final String BASE = "BufferedMetricsPublisher.base";
  private final MetricsPersistenceService persistenceService;

  @Inject
  public BufferedMetricsPublisher(CConfiguration cConf, @Named(BASE) MetricsPublisher publisher) {
    // TODO: define vars
    int persistingFrequencySeconds = cConf.getInt("abc");
    int maxMetricsCapacity = cConf.getInt("abc");
    this.metricValues = new ArrayBlockingQueue<>(maxMetricsCapacity);
    this.persistenceService = new MetricsPersistenceService(this.metricValues, publisher, persistingFrequencySeconds);
  }

  @Override
  public void publish(Collection<MetricValues> metrics) {
    for (MetricValues metricValues : metrics) {
      boolean inserted = this.metricValues.offer(metricValues);
      if (!inserted) {
        throw new RetryableException("Discarding metrics since queue is full");
      }
    }
    LOG.info("Added {} MetricValues to buffer", metrics.size());
  }

  @Override
  public void close() {
    if (this.persistenceService.isRunning()) {
      this.persistenceService.stop();
    }
  }
}
