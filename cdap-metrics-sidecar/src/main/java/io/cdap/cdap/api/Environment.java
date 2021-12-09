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

package io.cdap.cdap.api;

/**
 * An interface to abstract hidden dependency on System for getting environment variables
 * This also makes it easier to control env variable values during testing.
 */
public interface Environment {
  /**
   * @param key is the name of the env variable to fetch
   * @return the value of {@code key} env variable
   */
  String getVariable(String key);
}
