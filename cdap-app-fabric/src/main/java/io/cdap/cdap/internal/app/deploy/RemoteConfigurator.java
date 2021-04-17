/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.deploy;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.deploy.pipeline.ConfiguratorConfig;
import io.cdap.cdap.internal.app.dispatcher.ConfiguratorTask;
import io.cdap.cdap.internal.app.dispatcher.RunnableTaskRequest;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

public class RemoteConfigurator implements Configurator {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(RemoteConfigurator.class);
  private final ConfiguratorConfig config;

  final private RemoteClient remoteClientInternal;

  public RemoteConfigurator(ConfiguratorConfig config,
                            DiscoveryServiceClient discoveryServiceClient) {
    this.config = config;

    this.remoteClientInternal = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
                                                 new DefaultHttpRequestConfig(false),
                                                 String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));
  }

  public RemoteConfigurator(CConfiguration cConf, Id.Namespace appNamespace, Id.Artifact artifactId,
                            String appClassName, PluginFinder pluginFinder, ClassLoader artifactClassLoader,
                            String applicationName, String applicationVersion, String configString,
                            DiscoveryServiceClient discoveryServiceClient,
                            Location artifactLocation) {

    this.config = new ConfiguratorConfig(
      cConf, appNamespace,
      artifactId, appClassName,
      applicationName,
      applicationVersion,
      configString, artifactLocation.toURI());

    this.remoteClientInternal = new RemoteClient(discoveryServiceClient, Constants.Service.TASK_WORKER,
                                                 new DefaultHttpRequestConfig(false),
                                                 String.format("%s", Constants.Gateway.INTERNAL_API_VERSION_3));

  }

  @Override
  public ListenableFuture<ConfigResponse> config() {
    try {
      String param = GSON.toJson(config);
      LOG.error("Sending param: " + param);
      RunnableTaskRequest req = new RunnableTaskRequest(ConfiguratorTask.class.getName(), param);
      String reqBody = GSON.toJson(req);


      HttpRequest.Builder requestBuilder = remoteClientInternal.requestBuilder(
        HttpMethod.POST, String.format("/worker/run")).withBody(reqBody);
      HttpResponse response = remoteClientInternal.execute(requestBuilder.build());

//      URI uri = URI.create("http://cdap-meseifan-rbac-task-worker:11020");
//      LOG.error("Sending request");
//      HttpResponse httpResponse = HttpRequests.execute(
//        HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
//          .withBody(reqBody).build(),
//        new DefaultHttpRequestConfig(false));
      ByteArrayInputStream bis = new ByteArrayInputStream(response.getResponseBody());
      ObjectInput in = null;

      in = new ObjectInputStream(bis);
      Object o = in.readObject();
      return (ListenableFuture<ConfigResponse>) o;
    } catch (IOException | ClassNotFoundException ex) {
      return null;
    }
  }
}
