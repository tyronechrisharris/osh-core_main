/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore.system;

import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.system.ISystemDescStore.SystemField;
import org.sensorhub.api.system.ISystemWithDesc;


/**
 * <p>
 * Interface for data stores containing system SensorML descriptions
 * </p>
 *
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public interface ISystemDescStore extends IFeatureStoreBase<ISystemWithDesc, SystemField, SystemFilter>
{

    public static class SystemField extends FeatureField
    {
        public static final SystemField TYPE = new SystemField("type");
        public static final SystemField GENERAL_METADATA = new SystemField("metadata");
        
        public SystemField(String name)
        {
            super(name);
        }
    }
    
    
    @Override
    public default SystemFilter.Builder filterBuilder()
    {
        return new SystemFilter.Builder();
    }
    
    
    /**
     * Link this store to a datastream store to enable JOIN queries
     * @param dataStreamStore
     */
    public void linkTo(IDataStreamStore dataStreamStore);
    
}
