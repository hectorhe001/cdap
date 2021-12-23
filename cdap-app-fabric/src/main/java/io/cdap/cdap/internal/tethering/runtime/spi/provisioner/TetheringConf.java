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

package io.cdap.cdap.internal.tethering.runtime.spi.provisioner;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.internal.tethering.NamespaceAllocation;
import io.cdap.cdap.internal.tethering.PeerInfo;
import io.cdap.cdap.internal.tethering.TetheringStatus;
import io.cdap.cdap.internal.tethering.TetheringStore;
import io.cdap.cdap.metrics.collect.LocalMetricsCollectionService;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.tephra.runtime.TransactionModules;

import java.util.List;
import java.util.Map;

/**
 * Configuration for the Tethering Provisioner.
 */
public class TetheringConf {
  public static final String TETHERED_INSTANCE_PROPERTY = "tetheredInstanceName";
  public static final String TETHERED_NAMESPACE_PROPERTY = "tetheredNamespace";

  private final String tetheredInstanceName;
  private final String tetheredNamespace;

  private TetheringConf(String tetheredInstanceName, String tetheredNamespace) {
    this.tetheredInstanceName = tetheredInstanceName;
    this.tetheredNamespace = tetheredNamespace;
  }

  /**
   * Create the conf from a property map while also performing validation that tethered connection exists.
   */
  public static TetheringConf fromProperties(Map<String, String> properties) {
    String tetheredInstanceName = getString(properties, TETHERED_INSTANCE_PROPERTY);
    String tetheredNamespace = getString(properties, TETHERED_NAMESPACE_PROPERTY);

    Injector injector = createInjector();
    TetheringStore tetheringStore = new TetheringStore(injector.getInstance(TransactionRunner.class));
    checkTetheredConnection(tetheringStore, tetheredInstanceName, tetheredNamespace);

    return create(tetheredInstanceName, tetheredNamespace);
  }

  /**
   * Create the conf without performing validation.
   */
  public static TetheringConf create(String tetheredInstanceName, String tetheredNamespace) {
    return new TetheringConf(tetheredInstanceName, tetheredNamespace);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
    }
    return val;
  }

  @VisibleForTesting
  static void checkTetheredConnection(TetheringStore store, String tetheredInstanceName, String tetheredNamespace) {
    try {
      PeerInfo peerInfo = store.getPeer(tetheredInstanceName);
      if (peerInfo.getTetheringStatus() != TetheringStatus.ACCEPTED) {
        throw new IllegalArgumentException(String.format("Invalid config. Tethering connection to '%s' is status '%s'",
                                                         tetheredInstanceName, peerInfo.getTetheringStatus()));
      }
      List<NamespaceAllocation> namespaceAllocationList = peerInfo.getMetadata().getNamespaceAllocations();
      if (namespaceAllocationList.stream().noneMatch(alloc -> tetheredNamespace.equals(alloc.getNamespace()))) {
        throw new IllegalArgumentException(String.format("Invalid config. No tethering connection to '%s' '%s'",
                                                         tetheredInstanceName, tetheredNamespace));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static Injector createInjector() {
    CConfiguration cConf = CConfiguration.create();
    return Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getStandaloneModules(),
      new TransactionModules().getSingleNodeModules(),
      new TransactionExecutorModule(),
      new StorageModule(),
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
          expose(MetricsCollectionService.class);
        }
      });
  }

  public String getTetheredInstanceName() {
    return tetheredInstanceName;
  }

  public String getTetheredNamespace() {
    return tetheredNamespace;
  }
}
