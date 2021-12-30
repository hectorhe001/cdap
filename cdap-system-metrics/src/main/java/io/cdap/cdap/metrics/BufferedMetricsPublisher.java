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

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
  }

  @Override
  public void close() {
    if (this.persistenceService.isRunning()) {
      this.persistenceService.stop();
    }
  }

  private static class MetricsPersistenceService extends AbstractScheduledService {
    private final BlockingQueue<MetricValues> metricValues;
    private final MetricsPublisher metricsPublisher;
    private final int frequencySeconds;

    public MetricsPersistenceService(BlockingQueue<MetricValues> metricValues,
                                     MetricsPublisher metricsPublisher,
                                     int frequencySeconds) {
      this.metricValues = metricValues;
      this.metricsPublisher = metricsPublisher;
      this.frequencySeconds = frequencySeconds;
    }

    @Override
    protected void runOneIteration() {
      Collection<MetricValues> metrics = new ArrayList<>();
      this.metricValues.drainTo(metrics);
      try {
        this.metricsPublisher.publish(metrics);
      } catch (Exception e) {
        LOG.warn("Error while persisting metrics.", e);
      }
    }

    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(0, this.frequencySeconds, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws IOException {
      this.metricsPublisher.close();
      LOG.info("Shutting down MetricsPersistenceService has completed.");
    }
  }
}
