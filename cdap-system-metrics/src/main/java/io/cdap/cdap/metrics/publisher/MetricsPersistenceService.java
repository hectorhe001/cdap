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

import com.google.common.util.concurrent.AbstractScheduledService;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service that drains {@link MetricValues} from a buffer (in the form of a {@link BlockingQueue})
 * by writing to a {@link MetricsPublisher} at regular intervals.
 */
public class MetricsPersistenceService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsPersistenceService.class);
  private final BlockingQueue<MetricValues> metricValues;
  private final MetricsPublisher metricsPublisher;
  private final int frequencySeconds;
  private ScheduledExecutorService executor;

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
      return;
    }
    LOG.info("Drained {} metrics from buffer. Remaining capacity {}.",
             metrics.size(), metricValues.remainingCapacity());
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(this.frequencySeconds, this.frequencySeconds, TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("metrics-persistence-service"));
    return executor;
  }

  @Override
  protected void shutDown() throws IOException {
    if (executor != null) {
      executor.shutdownNow();
    }
    this.metricsPublisher.close();
    LOG.info("Shutting down MetricsPersistenceService has completed.");
  }
}
