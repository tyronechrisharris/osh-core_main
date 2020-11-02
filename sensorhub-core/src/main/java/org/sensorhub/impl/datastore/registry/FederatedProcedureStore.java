/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.obs.DataStreamFilter;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.procedure.IProcedureStore;
import org.sensorhub.api.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureStore.ProcedureField;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.vast.util.Asserts;
import org.vast.util.Bbox;


/**
 * <p>
 * Implementation of procedure store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedProcedureStore extends ReadOnlyDataStore<FeatureKey, IProcedureWithDesc, ProcedureField, ProcedureFilter> implements IProcedureStore
{
    DefaultDatabaseRegistry registry;
    FederatedObsDatabase db;
    
    
    FederatedProcedureStore(DefaultDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = registry;
        this.db = db;
    }


    @Override
    public String getDatastoreName()
    {
        return getClass().getSimpleName();
    }
    
    
    @Override
    public long getNumFeatures()
    {
        long count = 0;
        for (var db: registry.obsDatabases.values())
            count += db.getProcedureStore().getNumFeatures();
        return count;
    }


    @Override
    public long getNumRecords()
    {
        long count = 0;
        for (var db: registry.obsDatabases.values())
            count += db.getProcedureStore().getNumRecords();
        return count;
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        Bbox bbox = new Bbox();
        for (var db: registry.obsDatabases.values())
            bbox.add(db.getProcedureStore().getFeaturesBbox());
        return bbox;
    }
    
    
    protected FeatureKey ensureFeatureKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof FeatureKey, "key must be a FeatureKey");
        return (FeatureKey)obj;
    }


    @Override
    public boolean containsKey(Object obj)
    {
        FeatureKey key = ensureFeatureKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key.getInternalID());
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getProcedureStore().containsKey(new FeatureKey(dbInfo.entryID));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: registry.obsDatabases.values())
        {
            if (db.getProcedureStore().containsValue(value))
                return true;
        }
        
        return false;
    }
    
    
    /*
     * Convert to public IDs on the way out
     */
    protected FeatureId toPublicID(int databaseID, FeatureId id)
    {
        if (id == null)
            return null;
        
        long publicID = registry.getPublicID(databaseID, id.getInternalID());
        return new FeatureId(publicID, id.getUniqueID());
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected FeatureKey toPublicKey(int databaseID, FeatureKey k)
    {
        long publicID = registry.getPublicID(databaseID, k.getInternalID());
        return new FeatureKey(publicID, k.getValidStartTime());
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<FeatureKey, IProcedureWithDesc> toPublicEntry(int databaseID, Entry<FeatureKey, IProcedureWithDesc> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            e.getValue());
    }


    @Override
    public IProcedureWithDesc get(Object obj)
    {
        FeatureKey key = ensureFeatureKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key.getInternalID());
        if (dbInfo == null)
            return null;
        else
            return dbInfo.db.getProcedureStore().get(
                new FeatureKey(dbInfo.entryID, key.getValidStartTime()));
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(ProcedureFilter filter)
    {
        Map<Integer, LocalFilterInfo> dataStreamFilterDispatchMap = null;
        Map<Integer, LocalFilterInfo> parentFilterDispatchMap = null;
        Map<Integer, LocalFilterInfo> procFilterDispatchMap = new TreeMap<>();
        
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = ProcedureFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        // otherwise get dispatch map for datastreams and parent procedures
        if (filter.getDataStreamFilter() != null)
            dataStreamFilterDispatchMap = db.obsStore.dataStreamStore.getFilterDispatchMap(filter.getDataStreamFilter());
        
        if (filter.getParentFilter() != null)
            parentFilterDispatchMap = getFilterDispatchMap(filter.getParentFilter());
        
        // merge both maps
        if (dataStreamFilterDispatchMap != null)
        {
            for (var entry: dataStreamFilterDispatchMap.entrySet())
            {
                var dataStreamFilterInfo = entry.getValue();
                                
                var builder = ProcedureFilter.Builder
                    .from(filter)
                    .withDataStreams((DataStreamFilter)dataStreamFilterInfo.filter);
                
                var parentfilterInfo = parentFilterDispatchMap != null ? parentFilterDispatchMap.get(entry.getKey()) : null;
                if (parentfilterInfo != null)
                    builder.withParents((ProcedureFilter)parentfilterInfo.filter);
                    
                var filterInfo = new LocalFilterInfo();
                filterInfo.databaseID = dataStreamFilterInfo.databaseID;
                filterInfo.db = dataStreamFilterInfo.db;
                filterInfo.filter = builder.build();
                procFilterDispatchMap.put(entry.getKey(), filterInfo);
            }
        }
        
        if (parentFilterDispatchMap != null)
        {
            for (var entry: parentFilterDispatchMap.entrySet())
            {
                var parentFilterInfo = entry.getValue();
                
                // only process DBs not already processed in first loop above
                if (!procFilterDispatchMap.containsKey(entry.getKey()))
                {
                    var filterInfo = new LocalFilterInfo();
                    filterInfo.databaseID = parentFilterInfo.databaseID;
                    filterInfo.db = parentFilterInfo.db;
                    filterInfo.filter = ProcedureFilter.Builder.from(filter)
                        .withParents((ProcedureFilter)parentFilterInfo.filter)
                        .build();
                    procFilterDispatchMap.put(entry.getKey(), filterInfo);
                }
            }
        }
        
        if (!procFilterDispatchMap.isEmpty())
            return procFilterDispatchMap;
        else
            return null;
    }


    @Override
    public Stream<Entry<FeatureKey, IProcedureWithDesc>> selectEntries(ProcedureFilter filter, Set<ProcedureField> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getProcedureStore().selectEntries((ProcedureFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
        else
        {
            // otherwise scan all DBs
            return registry.obsDatabases.values().stream()
                .flatMap(db -> {
                    int dbID = db.getDatabaseID();
                    return db.getProcedureStore().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
    }
    
    
    @Override
    public FeatureKey add(IProcedureWithDesc feature)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public FeatureKey add(long parentId, IProcedureWithDesc value)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
    }

}
