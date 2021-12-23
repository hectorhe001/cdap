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

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.internal.tethering.NamespaceAllocation;
import io.cdap.cdap.internal.tethering.PeerAlreadyExistsException;
import io.cdap.cdap.internal.tethering.PeerInfo;
import io.cdap.cdap.internal.tethering.PeerMetadata;
import io.cdap.cdap.internal.tethering.TetheringStatus;
import io.cdap.cdap.internal.tethering.TetheringStore;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;

public class TetheringConfTest {

  private static final String PEER_1 = "peer1";
  private static final String PEER_2 = "peer2";
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";

  private static TetheringStore tetheringStore;
  private static TransactionManager txManager;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setUp() throws IOException, TopicAlreadyExistsException, PeerAlreadyExistsException {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class).in(Scopes.SINGLETON);
        }
      });
    tetheringStore = new TetheringStore(injector.getInstance(TransactionRunner.class));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    // Define all StructuredTable before starting any services that need StructuredTable
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    PeerMetadata metadata1 = new PeerMetadata(ImmutableList.of(new NamespaceAllocation(NAMESPACE_1, "1", "1Gi")),
                                              Collections.emptyMap());
    PeerMetadata metadata2 = new PeerMetadata(ImmutableList.of(new NamespaceAllocation(NAMESPACE_2, "2", "2Gi")),
                                              Collections.emptyMap());
    PeerInfo peer1 = new PeerInfo(PEER_1, "endpoint1", TetheringStatus.PENDING, metadata1);
    PeerInfo peer2 = new PeerInfo(PEER_2, "endpoint2", TetheringStatus.ACCEPTED, metadata2);
    tetheringStore.addPeer(peer1);
    tetheringStore.addPeer(peer2);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  @Test
  public void testCheckValidTetheredConnection() {
    TetheringConf.checkTetheredConnection(tetheringStore, PEER_2, NAMESPACE_2);
  }

  @Test
  public void testCheckPendingTetheredConnection() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Tethering connection to '%s' is status '%s'", PEER_1, TetheringStatus.PENDING));
    TetheringConf.checkTetheredConnection(tetheringStore, PEER_1, NAMESPACE_1);
  }

  @Test
  public void testCheckInvalidInstanceTetheredConnection() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Peer %s not found", "fake-peer"));
    TetheringConf.checkTetheredConnection(tetheringStore, "fake-peer", NAMESPACE_1);
  }

  @Test
  public void testCheckInvalidNamespaceTetheredConnection() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("No tethering connection to '%s' '%s'", PEER_2, NAMESPACE_1));
    TetheringConf.checkTetheredConnection(tetheringStore, PEER_2, NAMESPACE_1);
  }
}
