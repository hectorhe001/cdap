/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.metrics;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.*;


public class MetricsReporterHookTest {
    static final String TESTSERVICENAME = "test.Service";
    static final String TESTHANDLERNAME = "test.handler";
    static final String TESTMETHODNAME = "testMethod";

    @Test
    public void testReponseTimeCollection() throws InterruptedException {
        MetricsContext mockCollector = mock(MetricsContext.class);
        MetricsCollectionService mockCollectionService = mock(MetricsCollectionService.class);
        when(mockCollectionService.getContext(anyMap())).thenReturn(mockCollector);

        HttpRequest request = mock(HttpRequest.class);
        HandlerInfo handlerInfo = new HandlerInfo(TESTHANDLERNAME, TESTMETHODNAME);
        MetricsReporterHook hook = new MetricsReporterHook(mockCollectionService, TESTSERVICENAME);

        hook.preCall(request, null, handlerInfo);
        // tiny sleep to simulate an API call
        Thread.sleep(2);
        hook.postCall(request, HttpResponseStatus.OK, handlerInfo);

        verify(mockCollector).gauge(eq("response."+TESTSERVICENAME+"."+TESTMETHODNAME), anyInt());
    }
}
