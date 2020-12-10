/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore.procedure;

import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore.ProcedureField;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureWrapper;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Interface for data store containing procedure descriptions (i.e. SensorML
 * objects derived from AbstractProcess)
 * </p>
 *
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public interface IProcedureStore extends IFeatureStoreBase<IProcedureWithDesc, ProcedureField, ProcedureFilter>
{

    public static class ProcedureField extends FeatureField
    {
        public static final ProcedureField TYPE = new ProcedureField("type");
        public static final ProcedureField GENERAL_METADATA = new ProcedureField("metadata");
        public static final ProcedureField HISTORY = new ProcedureField("history");
        public static final ProcedureField MEMBERS = new ProcedureField("members");
        
        public ProcedureField(String name)
        {
            super(name);
        }
    }
    
    
    /**
     * Add a new procedure using its full description.<br/>
     * This method delegates to {@link IFeatureStoreBase#add(T)}
     * @param desc The full procedure description
     * @return The newly allocated key (internal ID)
     * @throws DataStoreException if a procedure with the same UID already exists
     * in this store
     */
    public default FeatureKey add(AbstractProcess desc) throws DataStoreException
    {
        return add(new ProcedureWrapper(desc));
    }
    
    
    /**
     * Add a new procedure using its full description and associate to its parent.<br/>
     * This method delegates to {@link IFeatureStoreBase#add(long, T)}
     * @param parentID Internal ID of parent procedure
     * @param desc The full procedure description
     * @return The newly allocated key (internal ID)
     * @throws DataStoreException if a procedure with the same UID already exists
     * in this store, or if the parent ID is unknown
     */
    public default FeatureKey add(long parentID, AbstractProcess desc) throws DataStoreException
    {
        return add(parentID, new ProcedureWrapper(desc));
    }
    
    
    @Override
    public default ProcedureFilter.Builder filterBuilder()
    {
        return new ProcedureFilter.Builder();
    }
    
    
    /**
     * Link this store to a datastream store to enable JOIN queries
     * @param dataStreamStore
     */
    public void linkTo(IDataStreamStore dataStreamStore);
    
}
