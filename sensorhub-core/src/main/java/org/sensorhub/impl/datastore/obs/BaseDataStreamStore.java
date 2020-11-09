/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.obs;

import java.time.Instant;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Base helper class for DataStreamStore implementations
 * </p>
 *
 * @author Alex Robin
 * @date Nov 3, 2020
 */
public abstract class BaseDataStreamStore implements IDataStreamStore
{
    protected static final String ERROR_BAD_KEY = "Key must be an instance of " + DataStreamKey.class.getSimpleName();
    protected static final String ERROR_EXISTING_DATASTREAM = "A datastream for the same procedure, output and validTime already exists";

    protected IProcedureStore procedureStore;
    
    
    protected abstract IObsStore getObsStore();
    protected abstract DataStreamKey generateKey(IDataStreamInfo dsInfo);
    protected abstract IDataStreamInfo put(DataStreamKey key, IDataStreamInfo dsInfo, boolean replace);
    
    
    @Override
    public synchronized DataStreamKey add(IDataStreamInfo dsInfo)
    {
        Asserts.checkNotNull(dsInfo, IDataStreamInfo.class);
        Asserts.checkNotNull(dsInfo.getProcedureID(), "procedureID");
        Asserts.checkNotNull(dsInfo.getOutputName(), "outputName");
        
        // check parent procedure exists in linked store
        if (procedureStore != null && procedureStore.getCurrentVersionKey(dsInfo.getProcedureID().getInternalID()) == null)
            throw new IllegalArgumentException("Unknown procedure with ID " + dsInfo.getProcedureID());
        
        // use valid time of parent procedure or current time if none was set
        if (dsInfo.getValidTime() == null)
        {
            TimeExtent validTime = null;
            
            if (procedureStore != null)
            {
                var fk = procedureStore.getCurrentVersionKey(dsInfo.getProcedureID().getInternalID());
                if (fk.getValidStartTime() != Instant.MIN)
                    validTime = TimeExtent.endNow(fk.getValidStartTime());
            }
            
            if (validTime == null)
                validTime = TimeExtent.endNow(Instant.now());
            
            // create new datastream obj with proper valid time
            dsInfo = DataStreamInfo.Builder.from(dsInfo)
                .withValidTime(validTime)
                .build();
        }

        // create key
        var newKey = generateKey(dsInfo);

        // add to store
        put(newKey, dsInfo, false);
        return newKey;
    }


    @Override
    public IDataStreamInfo put(DataStreamKey key, IDataStreamInfo dsInfo)
    {
        checkKey(key);
        Asserts.checkNotNull(dsInfo, IDataStreamInfo.class);
        Asserts.checkNotNull(dsInfo.getProcedureID(), "procedureID");
        Asserts.checkNotNull(dsInfo.getName(), "outputName");
        
        return put(key, dsInfo, true);
    }
    
    
    protected Stream<Long> selectProcedureIDs(ProcedureFilter filter)
    {
        if (filter.getInternalIDs() != null &&
            filter.getLocationFilter() == null )
        {
            // if only internal IDs were specified, no need to search the feature store
            return filter.getInternalIDs().stream();
        }
        else
        {
            Asserts.checkState(procedureStore != null, "No linked procedure store");
            
            // otherwise get all feature keys matching the filter from linked feature store
            // we apply the distinct operation to make sure the same feature is not
            // listed twice (it can happen when there exists several versions of the
            // same feature with different valid times)
            return procedureStore.selectKeys(filter)
                .map(k -> k.getInternalID())
                .distinct();
        }
    }
    
    
    protected DataStreamKey checkKey(Object key)
    {
        Asserts.checkNotNull(key, "key");
        Asserts.checkArgument(key instanceof DataStreamKey, ERROR_BAD_KEY);
        return (DataStreamKey)key;
    }
    
    
    @Override
    public void linkTo(IProcedureStore procedureStore)
    {
        Asserts.checkNotNull(procedureStore, IProcedureStore.class);
        
        if (this.procedureStore != procedureStore)
        {
            this.procedureStore = procedureStore;
            procedureStore.linkTo(this);
        }
    }

}
