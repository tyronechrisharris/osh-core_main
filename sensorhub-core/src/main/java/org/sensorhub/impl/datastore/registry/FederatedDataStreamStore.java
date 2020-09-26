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

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.obs.DataStreamFilter;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IDataStreamStore;
import org.sensorhub.api.obs.ObsFilter;
import org.sensorhub.api.obs.IDataStreamStore.DataStreamInfoField;
import org.sensorhub.api.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of datastream store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedDataStreamStore extends ReadOnlyDataStore<Long, IDataStreamInfo, DataStreamInfoField, DataStreamFilter> implements IDataStreamStore
{
    DefaultDatabaseRegistry registry;
    FederatedObsDatabase db;    
    
    
    FederatedDataStreamStore(DefaultDatabaseRegistry registry, FederatedObsDatabase db)
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
    public long getNumRecords()
    {
        long count = 0;
        for (var db: registry.obsDatabases.values())
            count += db.getObservationStore().getDataStreams().getNumRecords();
        return count;
    }


    @Override
    public int size()
    {
        return (int)getNumRecords();
    }


    @Override
    public boolean isEmpty()
    {
        return getNumRecords() == 0;
    }
    
    
    protected Long ensureDataStreamKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof Long, "key must be a Long");
        return (Long)obj;
    }


    @Override
    public boolean containsKey(Object obj)
    {
        Long key = ensureDataStreamKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key);
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getObservationStore().getDataStreams().containsKey(dbInfo.entryID);
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: registry.obsDatabases.values())
        {
            if (db.getObservationStore().getDataStreams().containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public IDataStreamInfo get(Object obj)
    {
        Long key = ensureDataStreamKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key);
        if (dbInfo != null)
        {
            IDataStreamInfo dsInfo = dbInfo.db.getObservationStore().getDataStreams().get(dbInfo.entryID);
            if (dsInfo != null)
                return toPublicValue(dbInfo.databaseID, dsInfo);
        }
        
        return null;
    }
    
    
    /*
     * Convert to public values on the way out
     */
    protected IDataStreamInfo toPublicValue(int databaseID, IDataStreamInfo dsInfo)
    {
        long procPublicID = registry.getPublicID(databaseID, dsInfo.getProcedureID().getInternalID());
        ProcedureId publicId = new ProcedureId(procPublicID, dsInfo.getProcedureID().getUniqueID());

        return DataStreamInfo.Builder.from(dsInfo)
            .withProcedure(publicId)
            .build();
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<Long, IDataStreamInfo> toPublicEntry(int databaseID, Entry<Long, IDataStreamInfo> e)
    {
        long publicID = registry.getPublicID(databaseID, e.getKey());
        return new AbstractMap.SimpleEntry<>(publicID, toPublicValue(databaseID, e.getValue()));
    }
    
    
    /*
     * Get dispatch map according to internal IDs used in filter
     */
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(DataStreamFilter filter)
    {
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = DataStreamFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getProcedureFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = db.procStore.getFilterDispatchMap(filter.getProcedureFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = DataStreamFilter.Builder
                        .from(filter)
                        .withProcedures((ProcedureFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getObservationFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = db.obsStore.getFilterDispatchMap(filter.getObservationFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = DataStreamFilter.Builder
                        .from(filter)
                        .withObservations((ObsFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }
    
    
    @Override
    public Stream<Entry<Long, IDataStreamInfo>> selectEntries(DataStreamFilter filter, Set<DataStreamInfoField> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getObservationStore().getDataStreams().selectEntries((DataStreamFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
        else
        {
            return registry.obsDatabases.values().stream()
                .flatMap(db -> {
                    int dbID = db.getDatabaseID();
                    return db.getObservationStore().getDataStreams().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
    }


    @Override
    public Set<Entry<Long, IDataStreamInfo>> entrySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<Entry<Long, IDataStreamInfo>> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getObservationStore().getDataStreams().entrySet().stream()
                            .map(e -> toPublicEntry(dbID, e));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedDataStreamStore.this.size();
            }        
        };
    }


    @Override
    public Set<Long> keySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<Long> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> db.getObservationStore().getDataStreams().keySet().stream()
                        .map(k -> registry.getPublicID(db.getDatabaseID(), k)))
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedDataStreamStore.this.size();
            }
        };
    }


    @Override
    public Collection<IDataStreamInfo> values()
    {
        return new AbstractCollection<>()
        {
            @Override
            public Iterator<IDataStreamInfo> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> db.getObservationStore().getDataStreams().values().stream())
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedDataStreamStore.this.size();
            }
        };
    }


    @Override
    public Long add(IDataStreamInfo dsInfo)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }

}
