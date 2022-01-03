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

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import javax.inject.Qualifier;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Class that uses the Decorator pattern to wrap another {@link MetricsPublisher}.
 * This class limits the rate at which metrics are sent to the wrapped publisher.
 */
public class BufferedMetricsPublisher extends AbstractMetricsPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(BufferedMetricsPublisher.class);
  private final BlockingQueue<MetricValues> metricsBuffer;
  private final MetricsPersistenceService persistenceService;

  @Inject
  public BufferedMetricsPublisher(CConfiguration cConf, @Base MetricsPublisher publisher) {
    int persistingFrequencySeconds =
      cConf.getInt(Constants.BufferedMetricsPublisher.PERSISTING_FREQUENCY_SECONDS);
    int bufferCapacity = cConf.getInt(Constants.BufferedMetricsPublisher.BUFFER_CAPACITY);
    this.metricsBuffer = new ArrayBlockingQueue<>(bufferCapacity);
    this.persistenceService = new MetricsPersistenceService(this.metricsBuffer, publisher, persistingFrequencySeconds);
    this.persistenceService.start();
  }

  @Override
  public void publish(Collection<MetricValues> metrics) {
    for (MetricValues metricValues : metrics) {
      boolean inserted = this.metricsBuffer.offer(metricValues);
      if (!inserted) {
        throw new RetryableException("Discarding metrics since queue is full");
      }
    }
    LOG.info("Added {} MetricValues to buffer", metrics.size());
  }

  public int getRemainingCapacity() {
    return this.metricsBuffer.remainingCapacity();
  }

  @Override
  public void close() {
    this.metricsBuffer.clear();
    if (this.persistenceService.isRunning()) {
      this.persistenceService.stopAndWait();
    }
    LOG.info("BufferedMetricsPublisher is closed.");
  }

  @Qualifier
  @Target({FIELD, PARAMETER, METHOD})
  @Retention(RUNTIME)
  public @interface Base {
  }
}
