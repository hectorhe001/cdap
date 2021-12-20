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

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.Environment;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.environment.SystemEnvironment;
import io.cdap.cdap.jmx.metrics.JMXMetricsCollector;
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
    System.out.println("Name of env: " + masterEnv.getName());
    if (masterEnv.getName() == "k8s") {
      System.out.println(((KubeMasterEnvironment) masterEnv).getPodInfo().getRuntimeClassName());
      System.out.println(((KubeMasterEnvironment) masterEnv).getPodInfo().getName());
      System.out.println(((KubeMasterEnvironment) masterEnv).getPodInfo().getContainerLabelName());
    }
    return Arrays.asList(
      new MessagingClientModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(Environment.class).to(SystemEnvironment.class);
        }
      }
    );
  }

  @Override
  protected void addServices(Injector injector, List<? super Service> services,
                             List<? super AutoCloseable> closeableResources,
                             MasterEnvironment masterEnv,
                             MasterEnvironmentContext masterEnvContext,
                             EnvironmentOptions options) {
    services.add(injector.getInstance(JMXMetricsCollector.class));
  }

  @Override
  protected LoggingContext getLoggingContext(EnvironmentOptions options) {
    return new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                     Constants.Logging.COMPONENT_NAME,
                                     System.getenv(Constants.JMXMetricsCollector.COMPONENT_NAME_ENV_VAR));
  }
}
