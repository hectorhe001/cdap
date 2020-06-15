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

import { WIDGET_FACTORY } from 'components/AbstractWidget/AbstractWidgetFactory';
import { GLOBALS } from 'services/global-constants';

export const PluginTypes = Object.keys(GLOBALS.pluginTypeToLabel).filter(
  (t) => t !== 'sqljoiner' && t !== 'batchjoiner' && t !== 'errortransform'
);

export const WIDGET_TYPES = Object.keys(WIDGET_FACTORY);
export const WIDGET_CATEGORY = ['plugin'];

export const SPEC_VERSION = '1.5';
