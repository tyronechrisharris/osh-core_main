/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.datastore.obs.IObsStore.ObsField;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsStats;
import org.sensorhub.impl.database.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.sensorhub.impl.datastore.MergeSortSpliterator;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
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
public class FederatedObsStore extends ReadOnlyDataStore<BigInteger, IObsData, ObsField, ObsFilter> implements IObsStore
{
    DefaultDatabaseRegistry registry;
    FederatedDataStreamStore dataStreamStore;
    FederatedObsDatabase db;
    
    
    FederatedObsStore(DefaultDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = registry;
        this.db = db;
        this.dataStreamStore = new FederatedDataStreamStore(registry, db);
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
    
    
    protected BigInteger ensureObsKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof BigInteger, "Key must be a BigInteger");
        return (BigInteger)obj;
    }
    
    
    /*
     * Convert to local keys on the way in
     */
    protected BigInteger toLocalKey(int databaseID, BigInteger key)
    {
        return registry.getLocalID(databaseID, key);
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected BigInteger toPublicKey(int databaseID, BigInteger k)
    {
        return registry.getPublicID(databaseID, k);
    }
    
    
    /*
     * Convert to public values on the way out
     */
    protected IObsData toPublicValue(int databaseID, IObsData obs)
    {
        long dsPublicId = registry.getPublicID(databaseID, obs.getDataStreamID());
        
        long foiPublicId = registry.getPublicID(databaseID, obs.getFoiID().getInternalID());
        FeatureId publicFoi = obs.hasFoi() ?
            IObsData.NO_FOI :
            new FeatureId(foiPublicId, obs.getFoiID().getUniqueID());
                
        /*return ObsData.Builder.from(obs)
            .withDataStream(dsPublicId)
            .withFoi(publicFoi)
            .build();*/
            
        // wrap original observation to return correct public IDs
        return new ObsDelegate(obs) {
            @Override
            public long getDataStreamID()
            {
                return dsPublicId;
            }

            @Override
            public FeatureId getFoiID()
            {
                return publicFoi;
            }            
        };
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<BigInteger, IObsData> toPublicEntry(int databaseID, Entry<BigInteger, IObsData> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            toPublicValue(databaseID, e.getValue()));
    }


    @Override
    public boolean containsKey(Object obj)
    {
        BigInteger key = ensureObsKey(obj);
        
        // use datastream public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key);
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getObservationStore().containsKey(
                toLocalKey(dbInfo.databaseID, key));
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
    public IObsData get(Object obj)
    {
        BigInteger key = ensureObsKey(obj);
        
        // use datastream public key to lookup database and local key
        var dbInfo = registry.getLocalDbInfo(key);
        if (dbInfo == null)
            return null;
        
        IObsData obs = dbInfo.db.getObservationStore().get(toLocalKey(dbInfo.databaseID, key));
        if (obs == null)
            return null;
        
        return toPublicValue(dbInfo.databaseID, obs);
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(ObsFilter filter)
    {
        Map<Integer, LocalFilterInfo> dataStreamFilterDispatchMap = null;
        Map<Integer, LocalFilterInfo> foiFilterDispatchMap = null;
        Map<Integer, LocalFilterInfo> obsFilterDispatchMap = new TreeMap<>();
        
        // use internal IDs if present
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = registry.getFilterDispatchMapBigInt(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = ObsFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.bigInternalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        // otherwise get dispatch map for datastreams and fois
        if (filter.getDataStreamFilter() != null)
            dataStreamFilterDispatchMap = dataStreamStore.getFilterDispatchMap(filter.getDataStreamFilter());
        
        if (filter.getFoiFilter() != null)
            foiFilterDispatchMap = db.foiStore.getFilterDispatchMap(filter.getFoiFilter());
        
        // merge both maps
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
    public Stream<Entry<BigInteger, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields)
    {
        final var obsIterators = new ArrayList<Spliterator<Entry<BigInteger, IObsData>>>(100);
        
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            filterDispatchMap.values().stream()
                .forEach(v -> {
                    int dbID = v.databaseID;
                    var obsStream = v.db.getObservationStore().selectEntries((ObsFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                    obsIterators.add(obsStream.spliterator());
                });
        }
        else
        {
            registry.obsDatabases.values().stream()
                .forEach(db -> {
                    int dbID = db.getDatabaseNum();
                    var obsStream = db.getObservationStore().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                    obsIterators.add(obsStream.spliterator());
                });
        }
        
        
        // stream and merge obs from all selected datastreams and time periods
        var mergeSortIt = new MergeSortSpliterator<Entry<BigInteger, IObsData>>(obsIterators,
            (e1, e2) -> e1.getValue().getPhenomenonTime().compareTo(e2.getValue().getPhenomenonTime()));         
               
        // stream output of merge sort iterator + apply limit        
        return StreamSupport.stream(mergeSortIt, false)
            .limit(filter.getLimit());
    }


    @Override
    public Stream<Long> selectObservedFois(ObsFilter filter)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getObservationStore().selectObservedFois((ObsFilter)v.filter)
                        .map(id -> registry.getPublicID(dbID, id));
                })
                .limit(filter.getLimit());
        }
        else
        {
            return registry.obsDatabases.values().stream()
                .flatMap(db -> {
                    int dbID = db.getDatabaseNum();
                    return db.getObservationStore().selectObservedFois(filter)
                        .map(id -> registry.getPublicID(dbID, id));
                })
                .limit(filter.getLimit());
        }
    }
    

    @Override
    public IDataStreamStore getDataStreams()
    {
        return dataStreamStore;
    }
    
    
    @Override
    public BigInteger add(IObsData obs)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void linkTo(IFoiStore foiStore)
    {
        throw new UnsupportedOperationException();        
    }

}
