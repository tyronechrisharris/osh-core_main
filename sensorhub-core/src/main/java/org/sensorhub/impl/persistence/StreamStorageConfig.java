/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.persistence;

import java.util.ArrayList;
import java.util.List;
import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.config.DisplayInfo.Required;
import org.sensorhub.api.persistence.StorageConfig;


public class StreamStorageConfig extends StorageConfig
{
    
    @Required
    @DisplayInfo(label="Storage Config", desc="Configuration of underlying storage")
    public StorageConfig storageConfig;
    
    
    @Required
    @DisplayInfo(label="Data Source ID", desc="Local ID of streaming data source which data will be store.")
    public String dataSourceID;
    
    
    @DisplayInfo(desc="Names of data source outputs that should not be saved to storage")
    public List<String> excludedOutputs = new ArrayList<>();
    
    
    @DisplayInfo(label="Automatic Purge Policy", desc="Policy for automatically purging stored data")
    public StorageAutoPurgeConfig autoPurgeConfig;
    

    @DisplayInfo(desc="Minimum period between database commits (in ms)")
    public int minCommitPeriod = 10000;
    
    
    @DisplayInfo(desc="Set to false to stop storing data of received events in underlying storage")
    public boolean processEvents = true;
    
    
    public StreamStorageConfig()
    {
        this.moduleClass = GenericStreamStorage.class.getCanonicalName();
    }


    @Override
    public void setStorageIdentifier(String name)
    {
        if (storageConfig != null)
            storageConfig.setStorageIdentifier(name);
    }
}
