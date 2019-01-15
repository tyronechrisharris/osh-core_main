/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.security.SecurityConfig;
import org.sensorhub.impl.service.ogc.OGCServiceConfig;
import org.sensorhub.impl.service.swe.OfferingList;


/**
 * <p>
 * Configuration class for the SPS service module
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Sep 6, 2013
 */
public class SPSServiceConfig extends OGCServiceConfig
{
        
    @DisplayInfo(desc="Set to true to enable transactional operation support")
    public boolean enableTransactional = false;
    
    
    @DisplayInfo(label="Offerings", desc="Configuration of connectors for SPS offerings")
    public OfferingList<SPSConnectorConfig> connectors = new OfferingList<>();
    
    
    @DisplayInfo(desc="Security related options")
    public SecurityConfig security = new SecurityConfig();
    
    
    public SPSServiceConfig()
    {
        this.moduleClass = SPSService.class.getCanonicalName();
        this.endPoint = "/sps";
    }

}
