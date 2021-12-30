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

import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsPublisher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

abstract class AbstractMetricsPublisher implements MetricsPublisher {
  @Override
  public void publish(Collection<MetricValue> metrics, Map<String, String> tags) throws Exception {
    Collection<MetricValues> metricValues = new ArrayList<>();
    for (MetricValue metric : metrics) {
      metricValues.add(new MetricValues(tags, metric.getName(),
                         TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                         metric.getValue(), metric.getType()));
    }
    this.publish(metricValues);
  }
}
