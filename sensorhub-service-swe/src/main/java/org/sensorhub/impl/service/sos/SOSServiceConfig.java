/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.config.DisplayInfo.Required;
import org.sensorhub.api.security.SecurityConfig;
import org.sensorhub.impl.datastore.view.ProcedureObsDatabaseViewConfig;
import org.sensorhub.impl.sensor.VirtualProcedureGroupConfig;
import org.sensorhub.impl.service.ogc.OGCServiceConfig;


/**
 * <p>
 * Configuration class for the SOS service module
 * </p>
 *
 * @author Alex Robin
 * @since Sep 7, 2013
 */
public class SOSServiceConfig extends OGCServiceConfig
{
    @Required
    @DisplayInfo(desc="Metadata of procedure group that will be created to contain all sensors "
        + "registered through this service. Only sensors in this group will be modifiable by this service")
    public VirtualProcedureGroupConfig virtualSensorGroup = null;
    
    
    @DisplayInfo(label="Database ID", desc="ID of database module used for persisting data received by this service. "
        + "If none is provided, new procedures registered through this service will be available on the hub, but "
        + "with no persistence guarantee across restarts. Only the latest observation from each datastream will be "
        + "available and older observations will be discarded")
    public String databaseID = null;


    @DisplayInfo(desc="Filtered view to select procedures exposed as read-only through this service")
    public ProcedureObsDatabaseViewConfig exposedResources = null;


    @DisplayInfo(desc="Custom provider configurations. ")
    public LinkedHashSet<SOSProviderConfig> providerConfigurations = new LinkedHashSet<>();


    @DisplayInfo(desc="Mapping of custom formats mime-types to custom serializer classes")
    public List<SOSCustomFormatConfig> customFormats = new ArrayList<>();


    @DisplayInfo(desc="Security related options")
    public SecurityConfig security = new SecurityConfig();


    @DisplayInfo(desc="Set to true to enable transactional operation support")
    public boolean enableTransactional = false;


    @DisplayInfo(label="Max Observations Returned", desc="Maximum number of observations returned "
        + " by a historical GetObservation request (for each selected offering)")
    public int maxObsCount = 100;


    @DisplayInfo(label="Max Records Returned", desc="Maximum number of result records returned by a historical GetResult request")
    public int maxRecordCount = 100000;


    @DisplayInfo(desc="Maximum number of FoI IDs listed in capabilities")
    public int maxFois = 10;
    
    
    @DisplayInfo(desc="Default live time-out for new offerings created via SOS-T")
    public double defaultLiveTimeout = 600.0;


    @DisplayInfo(desc="Time-out period after which a template ID reserved using InsertResultTemplate "
        + "will expire if not used in InsertResult requests (in seconds)")
    public int templateTimeout = 600;


    public SOSServiceConfig()
    {
        this.moduleClass = SOSService.class.getCanonicalName();
        this.endPoint = "/sos";
    }
}
