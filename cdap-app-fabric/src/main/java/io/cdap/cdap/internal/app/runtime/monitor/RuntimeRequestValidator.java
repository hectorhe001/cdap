/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.netty.handler.codec.http.HttpRequest;

/**
 * Defines validation of incoming requests from program runtime.
 */
public interface RuntimeRequestValidator {

  /**
   * Validates that the program is not in end state, if so returns BadRequestException.
   * Otherwise returns the program status and any information associated with it.
   *
   * @param programRunId the program run id where the request is coming from
   * @param request the http request from the program runtime
   * @throws BadRequestException if the request is not valid
   * @throws AccessException if the request doesn't have a valid Authorization header
   * @return RunRecordDetail if the programRunId was found
   */
  RunRecordStatus checkProgramRunStatus(ProgramRunId programRunId, HttpRequest request) throws BadRequestException,
    AccessException;
}
