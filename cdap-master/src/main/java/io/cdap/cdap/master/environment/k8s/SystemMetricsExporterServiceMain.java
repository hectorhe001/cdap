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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.metrics.jmx.JMXMetricsCollectorFactory;
import io.cdap.cdap.app.guice.SystemMetricsExporterModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.proto.id.NamespaceId;

import java.util.Arrays;
import java.util.List;

/**
 * The main class to run services for exporting system metrics.
 */
public class SystemMetricsExporterServiceMain extends AbstractServiceMain<EnvironmentOptions> {

  /**
   * Main entry point
   */
  public static void main(String[] args) throws Exception {
    main(SystemMetricsExporterServiceMain.class, args);
  }

  @Override
  protected List<Module> getServiceModules(MasterEnvironment masterEnv,
                                           EnvironmentOptions options,
                                           CConfiguration cConf) {
    return Arrays.asList(
      // required by some module added by super class
      new MessagingClientModule(),
      new SystemMetricsExporterModule()
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    String podName = masterEnv.getProperty(Constants.KubeMasterEnvironment.PODNAME);
    String componentName = getComponentName(podName);
    services.add(injector.getInstance(JMXMetricsCollectorFactory.class).create(componentName));
  }

  @VisibleForTesting
  private String getComponentName(String podName) {
    // podName is of the format "cdap-xxx-<servicename>-<int>" for statefulsets
    String[] parts = podName.split("-");
    int numParts = parts.length;
    if (numParts < 3) {
      throw new IllegalArgumentException("podName is not of format cdap-xxx-<servicename>-<int>");
    }
    try {
      Integer.parseInt(parts[numParts - 1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("podName is not of format cdap-xxx-<servicename>-<int>");
    }
    return parts[numParts - 2];
  }

  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     Constants.Service.SYSTEM_METRICS_EXPORTER);
  }
}
