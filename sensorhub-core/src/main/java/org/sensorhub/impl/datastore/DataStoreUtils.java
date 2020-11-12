/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore;

import java.time.Instant;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Helper methods to implement datastores
 * </p>
 *
 * @author Alex Robin
 * @date Nov 10, 2020
 */
public class DataStoreUtils
{
    public static final String ERROR_INVALID_DATASTREAM_KEY = "Key must be an instance of " + DataStreamKey.class.getSimpleName();
    public static final String ERROR_EXISTING_DATASTREAM = "Datastore already contains datastream for the same procedure, output and validTime";
    
    public static final String ERROR_INVALID_FEATURE_KEY = "Key must be an instance of " + FeatureKey.class.getSimpleName();
    public static final String ERROR_EXISTING_FEATURE = "Datastore already contains feature with the same UID: ";
    public static final String ERROR_EXISTING_FEATURE_VERSION = "Datastore already contains feature with the same UID and validTime";
    public static final String ERROR_UNKNOWN_PARENT_FEATURE = "Unknown parent feature: ";
    
    
    public static long checkInternalID(long internalID)
    {
        Asserts.checkArgument(internalID > 0, "ID must be > 0");
        return internalID;
    }
    
    
    //////////////////////////////////////
    // Helper methods for feature stores
    //////////////////////////////////////
    
    public static FeatureKey checkFeatureKey(Object key)
    {
        Asserts.checkNotNull(key, FeatureKey.class);
        Asserts.checkArgument(key instanceof FeatureKey, ERROR_INVALID_FEATURE_KEY);
        return (FeatureKey)key;
    }
    
    public static String checkFeatureObject(IFeature f)
    {
        Asserts.checkNotNull(f, IFeature.class);
        return checkUniqueID(f.getUniqueIdentifier());
    }
    
    public static String checkUniqueID(String uid)
    {
        return Asserts.checkNotNull(uid, "uniqueID");
    }
    
    public static void checkParentFeatureExists(IFeatureStoreBase<?,?,?> dataStore, long parentID)
    {
        if (parentID != 0 && !dataStore.contains(parentID))
            throw new IllegalArgumentException(DataStoreUtils.ERROR_UNKNOWN_PARENT_FEATURE + parentID);
    }
    
    
    //////////////////////////////////////////
    // Helpers methods for datastream stores
    //////////////////////////////////////////
    
    public static DataStreamKey checkDataStreamKey(Object key)
    {
        Asserts.checkNotNull(key, DataStreamKey.class);
        Asserts.checkArgument(key instanceof DataStreamKey, ERROR_INVALID_DATASTREAM_KEY);
        return (DataStreamKey)key;
    }
    
    
    public static void checkDataStreamInfo(IProcedureStore procedureStore, IDataStreamInfo dsInfo)
    {
        Asserts.checkNotNull(dsInfo, IDataStreamInfo.class);
        Asserts.checkNotNull(dsInfo.getProcedureID(), "procedureID");
        Asserts.checkNotNull(dsInfo.getOutputName(), "outputName");
        checkParentProcedureExists(procedureStore, dsInfo);
    }
    
    
    public static void checkParentProcedureExists(IProcedureStore procedureStore, IDataStreamInfo dsInfo)
    {
        var procID = dsInfo.getProcedureID().getInternalID();
        if (procedureStore != null && procedureStore.getCurrentVersionKey(procID) == null)
            throw new IllegalArgumentException("Unknown parent procedure: " + procID);
    }
    
    
    public static IDataStreamInfo ensureValidTime(IProcedureStore procedureStore, IDataStreamInfo dsInfo)
    {
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
            return DataStreamInfo.Builder.from(dsInfo)
                .withValidTime(validTime)
                .build();
        }
        
        return dsInfo;
    }
    
    
    //////////////////////////////////////////
    // Helpers methods for JOIN operations
    //////////////////////////////////////////
        
    public static <V extends IFeature, F extends FeatureFilterBase<? super V>> Stream<Long> selectFeatureIDs(IFeatureStoreBase<V,?,F> featureStore, F filter)
    {
        if (filter.getInternalIDs() != null)
        {
            // if only internal IDs were specified, no need to search the linked datastore
            return filter.getInternalIDs().stream();
        }
        else
        {
            Asserts.checkState(featureStore != null, "No linked feature store");
            
            // otherwise get all feature keys matching the filter from linked datastore
            // we apply the distinct operation to make sure the same feature is not
            // listed twice (it can happen when there exists several versions of the 
            // same feature with different valid times)
            return featureStore.selectKeys(filter)
                .map(k -> k.getInternalID())
                .distinct();
        }
    }
    
    
    public static Stream<Long> selectProcedureIDs(IProcedureStore procedureStore, ProcedureFilter filter)
    {
        if (filter.getInternalIDs() != null)
        {
            // if only internal IDs were specified, no need to search the linked datastore
            return filter.getInternalIDs().stream();
        }
        else
        {
            Asserts.checkState(procedureStore != null, "No linked procedure store");
            
            // otherwise get all feature keys matching the filter from linked datastore
            // we apply the distinct operation to make sure the same feature is not
            // listed twice (it can happen when there exists several versions of the
            // same feature with different valid times)
            return procedureStore.selectKeys(filter)
                .map(k -> k.getInternalID())
                .distinct();
        }
    }
    
    
    public static Stream<Long> selectDataStreamIDs(IDataStreamStore dataStreamStore, DataStreamFilter filter)
    {
        if (filter.getInternalIDs() != null)
        {
            // if only internal IDs were specified, no need to search the linked datastore
            return filter.getInternalIDs().stream();
        }
        else
        {
            Asserts.checkState(dataStreamStore != null, "No linked datastream store");
            
            // otherwise get all datastream keys matching the filter from linked datastore
            return dataStreamStore.selectKeys(filter)
                .map(k -> k.getInternalID());
        }
    }
    
    
    public static Stream<IDataStreamInfo> selectDataStreams(IDataStreamStore dataStreamStore, DataStreamFilter filter)
    {
        Asserts.checkState(dataStreamStore != null, "No linked datastream store");            
        return dataStreamStore.select(filter);
    }
}
