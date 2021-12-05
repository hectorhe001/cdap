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

package io.cdap.cdap.jmx.metrics.guice;

import com.google.inject.AbstractModule;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;

public class JMXMetricsCollectorTestModule extends AbstractModule {

  private final CConfiguration cConf;
  private final MetricsCollectionService metricsService;

  public JMXMetricsCollectorTestModule(CConfiguration cConf, MetricsCollectionService metricsService) {
    this.cConf = cConf;
    this.metricsService = metricsService;
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).toInstance(cConf);
    bind(MetricsCollectionService.class).toInstance(metricsService);
  }
}
