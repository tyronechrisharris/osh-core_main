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
import java.util.TreeMap;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FoiFilter;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.ObsData;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.api.datastore.ObsKey;
import org.sensorhub.api.datastore.ObsStats;
import org.sensorhub.api.datastore.ObsStatsQuery;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalDatabaseInfo;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of observation store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedObsStore extends ReadOnlyDataStore<ObsKey, ObsData, ObsFilter> implements IObsStore
{
    DefaultDatabaseRegistry registry;
    FederatedDataStreamStore dataStreamStore;
    FederatedFoiStore foiStore;
    
    
    FederatedObsStore(DefaultDatabaseRegistry registry)
    {
        this.registry = registry;
        this.dataStreamStore = new FederatedDataStreamStore(registry);
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
            count += db.getObservationStore().getNumRecords();
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
    
    
    protected ObsKey ensureObsKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof ObsKey, "must be an ObsKey");
        return (ObsKey)obj;
    }
    
    
    protected ObsKey toLocalObsKey(LocalDatabaseInfo dbInfo, ObsKey key)
    {
        FeatureId foiID = new FeatureId(
            registry.getLocalID(key.getFoiID().getInternalID()),
            key.getFoiID().getUniqueID());
        
        return new ObsKey(dbInfo.entryID, foiID, key.getResultTime(), key.getPhenomenonTime());
    }


    @Override
    public boolean containsKey(Object obj)
    {
        ObsKey key = ensureObsKey(obj);
        
        // use datastream public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key.getDataStreamID());
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getObservationStore().containsKey(
                toLocalObsKey(dbInfo, key));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: registry.obsDatabases.values())
        {
            if (db.getObservationStore().containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public ObsData get(Object obj)
    {
        ObsKey key = ensureObsKey(obj);
        
        // use datastream public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key.getDataStreamID());
        if (dbInfo == null)
            return null;
        else
            return dbInfo.db.getObservationStore().get(
                toLocalObsKey(dbInfo, key));
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<ObsKey, ObsData> toPublicEntry(int databaseID, Entry<ObsKey, ObsData> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            e.getValue());
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected ObsKey toPublicKey(int databaseID, ObsKey k)
    {
        long dataStreamID = registry.getPublicID(databaseID, k.getDataStreamID());
        
        long internalFoiID = k.getFoiID().getInternalID();
        FeatureId foiID = internalFoiID == 0 ? ObsKey.NO_FOI : new FeatureId(
            registry.getPublicID(databaseID, k.getFoiID().getInternalID()),
            k.getFoiID().getUniqueID());
        
        return new ObsKey(dataStreamID, foiID, k.getResultTime(), k.getPhenomenonTime());
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(ObsFilter filter)
    {
        Map<Integer, LocalFilterInfo> dataStreamFilterDispatchMap = null, foiFilterDispatchMap = null;
        
        // get dispatch map for datastreams and fois
        if (filter.getDataStreamFilter() != null)
            dataStreamFilterDispatchMap = dataStreamStore.getFilterDispatchMap(filter.getDataStreamFilter());
        
        if (filter.getFoiFilter() != null)
            foiFilterDispatchMap = foiStore.getFilterDispatchMap(filter.getFoiFilter());
        
        // merge both maps
        Map<Integer, LocalFilterInfo> obsFilterDispatchMap = new TreeMap<>();
        if (dataStreamFilterDispatchMap != null)
        {
            for (var entry: dataStreamFilterDispatchMap.entrySet())
            {
                var dataStreamFilterInfo = entry.getValue();
                                
                var builder = ObsFilter.Builder
                    .from(filter)
                    .withDataStreams((DataStreamFilter)dataStreamFilterInfo.filter);
                
                var foifilterInfo = foiFilterDispatchMap != null ? foiFilterDispatchMap.get(entry.getKey()) : null;
                if (foifilterInfo != null)
                    builder.withFois((FoiFilter)foifilterInfo.filter);
                    
                var filterInfo = new LocalFilterInfo();
                filterInfo.databaseID = dataStreamFilterInfo.databaseID;
                filterInfo.db = dataStreamFilterInfo.db;
                filterInfo.filter = builder.build();
                obsFilterDispatchMap.put(entry.getKey(), filterInfo);
            }
        }
        
        if (foiFilterDispatchMap != null)
        {
            for (var entry: foiFilterDispatchMap.entrySet())
            {
                var foiFilterInfo = entry.getValue();
                
                // only process DBs not already processed in first loop above
                if (!obsFilterDispatchMap.containsKey(entry.getKey()))
                {
                    var filterInfo = new LocalFilterInfo();
                    filterInfo.databaseID = foiFilterInfo.databaseID;
                    filterInfo.db = foiFilterInfo.db;
                    filterInfo.filter = ObsFilter.Builder.from(filter)
                        .withFois((FoiFilter)foiFilterInfo.filter)
                        .build();
                    obsFilterDispatchMap.put(entry.getKey(), filterInfo);
                }
            }
        }
        
        if (!obsFilterDispatchMap.isEmpty())
            return obsFilterDispatchMap;
        else
            return null;
    }


    @Override
    public Stream<Entry<ObsKey, ObsData>> selectEntries(ObsFilter filter)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getObservationStore().selectEntries((ObsFilter)v.filter)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
        else
        {
            return registry.obsDatabases.values().stream()
                .flatMap(db -> {
                    int dbID = db.getDatabaseID();
                    return db.getObservationStore().selectEntries(filter)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
    }


    @Override
    public Set<Entry<ObsKey, ObsData>> entrySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<Entry<ObsKey, ObsData>> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getObservationStore().entrySet().stream()
                            .map(e -> toPublicEntry(dbID, e));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedObsStore.this.size();
            }        
        };
    }


    @Override
    public Set<ObsKey> keySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<ObsKey> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getObservationStore().keySet().stream()
                            .map(k -> toPublicKey(dbID, k));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedObsStore.this.size();
            }        
        };
    }


    @Override
    public Collection<ObsData> values()
    {
        return new AbstractCollection<>()
        {
            @Override
            public Iterator<ObsData> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> db.getObservationStore().values().stream())
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedObsStore.this.size();
            }        
        };
    }


    @Override
    public IDataStreamStore getDataStreams()
    {
        return dataStreamStore;
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
