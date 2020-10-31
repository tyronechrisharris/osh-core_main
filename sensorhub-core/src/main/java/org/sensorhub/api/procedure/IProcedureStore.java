/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.feature.IFeatureStoreBase;
import org.sensorhub.api.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.procedure.IProcedureStore.ProcedureField;
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
     */
    public default FeatureKey add(AbstractProcess desc)
    {
        return add(new ProcedureWrapper(desc));
    }
    
    
    /**
     * Add a new procedure using its full description and associate to its parent.<br/>
     * This method delegates to {@link IFeatureStoreBase#add(long, T)}
     * @param parentID Internal ID of parent procedure
     * @param desc The full procedure description
     * @return The newly allocated key (internal ID)
     */
    public default FeatureKey add(long parentID, AbstractProcess desc)
    {
        return add(parentID, new ProcedureWrapper(desc));
    }
    
    
    @Override
    public default ProcedureFilter.Builder filterBuilder()
    {
        return new ProcedureFilter.Builder();
    }
    
    
    /**
     * Link this store to an observation store to enable JOIN queries
     * @param obsStore
     */
    public void linkTo(IObsStore obsStore);
    
}
