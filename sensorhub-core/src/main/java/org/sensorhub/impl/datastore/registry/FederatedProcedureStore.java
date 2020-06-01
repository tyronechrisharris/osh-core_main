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
import org.sensorhub.api.common.FeatureId;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.FeatureFilter;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureFilter;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureDescStore;
import org.sensorhub.api.procedure.IProcedureDescStore.ProcedureField;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Implementation of procedure store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedProcedureStore extends ReadOnlyDataStore<FeatureKey, AbstractProcess, ProcedureField, IFeatureFilter> implements IProcedureDescStore
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
    public int size()
    {
        return (int)getNumRecords();
    }


    @Override
    public boolean isEmpty()
    {
        return getNumRecords() == 0;
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
    protected Entry<FeatureKey, AbstractProcess> toPublicEntry(int databaseID, Entry<FeatureKey, AbstractProcess> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            e.getValue());
    }


    @Override
    public AbstractProcess get(Object obj)
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
        if (filter.getInternalIDs() != null && filter.getInternalIDs().isSet())
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs().getSet());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = ProcedureFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getDataStreamFilter() != null)
        {
            // delegate to datastream store handle datastream filter dispatch map
            var filterDispatchMap = db.obsStore.dataStreamStore.getFilterDispatchMap(filter.getDataStreamFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = ProcedureFilter.Builder
                        .from(filter)
                        .withDataStreams((DataStreamFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(FeatureFilter filter)
    {
        if (filter.getInternalIDs() != null && filter.getInternalIDs().isSet())
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs().getSet());            
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = FeatureFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(IFeatureFilter filter)
    {
        if (filter instanceof ProcedureFilter)
            return getFilterDispatchMap((ProcedureFilter)filter);
        else
            return getFilterDispatchMap((FeatureFilter)filter);
    }


    @Override
    public Stream<Entry<FeatureKey, AbstractProcess>> selectEntries(IFeatureFilter filter, Set<ProcedureField> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getProcedureStore().selectEntries((FeatureFilter)v.filter, fields)
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
    public Set<Entry<FeatureKey, AbstractProcess>> entrySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<Entry<FeatureKey, AbstractProcess>> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getProcedureStore().entrySet().stream()
                            .map(e -> toPublicEntry(dbID, e));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedProcedureStore.this.size();
            }        
        };
    }


    @Override
    public Set<FeatureKey> keySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<FeatureKey> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getProcedureStore().keySet().stream()
                            .map(k -> toPublicKey(dbID, k));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedProcedureStore.this.size();
            }        
        };
    }


    @Override
    public Collection<AbstractProcess> values()
    {
        return new AbstractCollection<>()
        {
            @Override
            public Iterator<AbstractProcess> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> db.getProcedureStore().values().stream())
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedProcedureStore.this.size();
            }        
        };
    }
    
    
    @Override
    public FeatureKey add(AbstractProcess feature)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public FeatureKey addVersion(AbstractProcess feature)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
    }

}
