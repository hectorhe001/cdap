/*
 * Copyright © 2022 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class SystemMetricsExporterServiceMainTest {
  @Test
  public void testGetComponentName() {
    // Ensure valid pod names are parsed correctly
    Assert.assertEquals(SystemMetricsExporterServiceMain
                          .getComponentName("cdap-instance-abc-appfabric-0"), "appfabric");
    Assert.assertEquals(SystemMetricsExporterServiceMain
                          .getComponentName("cdap-instance-abc-app-fabric-0"), "fabric");
    Assert.assertEquals(SystemMetricsExporterServiceMain
                          .getComponentName("cdap-abc-0121"), "abc");
    // Ensure exceptions are thrown for invalid pod names
    try {
      SystemMetricsExporterServiceMain.getComponentName("appfabric-0");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // no-op
    }

    try {
      SystemMetricsExporterServiceMain.getComponentName("appfabric-a0");
      Assert.fail("Expected IlegalArguementException, got no Exception thrown.");
    } catch (IllegalArgumentException e) {
      // no-op
    }

    try {
      SystemMetricsExporterServiceMain.getComponentName("appfabric");
      Assert.fail("Expected IlegalArguementException, got no Exception thrown.");
    } catch (IllegalArgumentException e) {
      // no-op
    }

    try {
      SystemMetricsExporterServiceMain.getComponentName("xya-abc");
      Assert.fail("Expected IlegalArguementException, got no Exception thrown.");
    } catch (IllegalArgumentException e) {
      // no-op
    }

    try {
      SystemMetricsExporterServiceMain.getComponentName("");
      Assert.fail("Expected IlegalArguementException, got no Exception thrown.");
    } catch (IllegalArgumentException e) {
      // no-op
    }
  }
}
