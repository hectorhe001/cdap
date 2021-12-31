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


import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BufferedMetricsPublisherTest extends AbstractPublisherTest {

  @Mock
  private MetricsPublisher publisher;

  @Before
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
  }

  @Test(expected = RetryableException.class)
  public void testBufferOverflow() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.BufferedMetricsPublisher.BUFFER_CAPACITY, 5);
    cConf.setInt(Constants.BufferedMetricsPublisher.PERSISTING_FREQUENCY_SECONDS, 500);
    BufferedMetricsPublisher bufferedPublisher = new BufferedMetricsPublisher(cConf, publisher);
    try {
      bufferedPublisher.publish(getMockMetricArray(5), new TreeMap<>());
    } catch (Exception e) {
      // fail test on premature exception
      bufferedPublisher.close();
      return;
    }
    bufferedPublisher.publish(getMockMetricArray(1), new TreeMap<>());
    bufferedPublisher.close();
  }

  @Test
  public void testBufferDrain() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.BufferedMetricsPublisher.BUFFER_CAPACITY, 5);
    cConf.setInt(Constants.BufferedMetricsPublisher.PERSISTING_FREQUENCY_SECONDS, 1);
    BufferedMetricsPublisher bufferedPublisher = new BufferedMetricsPublisher(cConf, publisher);
    for (int i = 0; i < 5; ++i) {
      bufferedPublisher.publish(getMockMetricArray(1), new TreeMap<>());
    }
    Assert.assertEquals(0, bufferedPublisher.getRemainingCapacity());
    Tasks.waitFor(5, () -> bufferedPublisher.getRemainingCapacity(), 2, TimeUnit.SECONDS);
    // Ensure decorated publisher is called at least once.
    verify(publisher, times(1)).publish(argThat(new IsListOfFiveElements()));
    // Ensure decorated publisher is called at most twice.
    verify(publisher, atMost(2)).publish(any());
    // Should be able to publish without overflow
    bufferedPublisher.publish(getMockMetricArray(5), new TreeMap<>());
    bufferedPublisher.close();
    verify(publisher, times(1)).close();
  }

  class IsListOfFiveElements extends ArgumentMatcher<Collection> {
    public boolean matches(Object list) {
      return ((Collection) list).size() == 5;
    }
  }
}