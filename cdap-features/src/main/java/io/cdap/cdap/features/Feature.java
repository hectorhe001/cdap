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

package io.cdap.cdap.features;

import io.cdap.cdap.api.PlatformInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines Features Flags to be used in CDAP.
 * Features take the version that they were introduced as a first parameter. Optionally they can take a
 * second paramenter to define their default behavior if they are not present in configuration.
 * By default features default to enabled after they are introduced, and disabled before they were introduced
 */
public enum Feature {
  ALLOW_FEATURE_FLAGS("6.6.0");
  
  public static final String FEATURE_FLAG_PREFIX = "feature.";
  private final PlatformInfo.Version versionIntroduced;
  private final boolean defaultAfterDeployment;
  private final String featureFlagString;

  Feature(String versionIntroduced) {
    this(versionIntroduced, true);
  }

  Feature(String versionDeployed, boolean defaultAfterDeployment) {
    this.featureFlagString = FEATURE_FLAG_PREFIX + this.name().toLowerCase().replace('_', '.') + ".enabled";
    this.versionIntroduced = new PlatformInfo.Version(versionDeployed);
    this.defaultAfterDeployment = defaultAfterDeployment;
  }

  public boolean isEnabled(Map<String, String> configuration) {
    String featureFlagValue = configuration.get(featureFlagString);
    if (featureFlagValue == null) {
      return getDefaultValue();
    }
    return convertStringToBoolean(featureFlagValue);
  }

  public String getFeatureFlagString() {
    return featureFlagString;
  }

  public static Map<String, String> extractFeatureFlags(Map<String, String> conf) {
    Map<String, String> featureFlags = new HashMap<>();
    for (String name : conf.keySet()) {
      if (name.startsWith(Feature.FEATURE_FLAG_PREFIX)) {
        String value = conf.get(name);
        if (!(("true".equals(value) || ("false".equals(value))))) {
          throw new IllegalArgumentException("Configured flag is not a valid boolean: name="
                                               + name + ", value=" + value);
        }
        featureFlags.put(name, value);
      }
    }
    return featureFlags;
  }

  private boolean convertStringToBoolean(String featureFlagValue) {
    return featureFlagValue.equals("true");
  }

  private boolean getDefaultValue() {
    if (PlatformInfo.getVersion().compareTo(versionIntroduced) <= 0) {
      return false;
    } else {
      return defaultAfterDeployment;
    }
  }




}
