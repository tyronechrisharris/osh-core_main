/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore;

import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.datastore.IHistoricalObsAutoPurgePolicy;


/**
 * <p>
 * Base configuration for automatic purge policies
 * </p>
 *
 * @author Alex Robin
 * @since Sep 23, 2019
 */
public abstract class HistoricalObsAutoPurgeConfig
{
    
    @DisplayInfo(desc="Uncheck to disable auto-purge temporarily")
    public boolean enabled = true;
    
    
    @DisplayInfo(label="Purge Execution Period", desc="Execution period of the purge policy (in seconds)")
    public double purgePeriod = 3600.0;

    
    public abstract IHistoricalObsAutoPurgePolicy getPolicy();
}
