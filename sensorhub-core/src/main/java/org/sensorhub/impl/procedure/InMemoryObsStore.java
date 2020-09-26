/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUObsData WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import java.math.BigInteger;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.RangeFilter;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.IDataStreamStore;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.obs.ObsFilter;
import org.sensorhub.api.obs.ObsStats;
import org.sensorhub.api.obs.ObsStatsQuery;
import org.sensorhub.api.procedure.IProcedureDescStore;
import com.google.common.collect.Range;


/**
 * <p>
 * In-memory implementation of an observation store backed by a {@link java.util.NavigableMap}.
 * This implementation is only used to store the latest procedure state and
 * thus only stores the latest observation of each data stream.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryObsStore extends InMemoryDataStore implements IObsStore
{
    static final RangeFilter<Instant> ALL_TIMES_FILTER = new RangeFilter.Builder<Instant>()
        .withRange(Instant.MIN, Instant.MAX)
        .build();
    
    ConcurrentMap<BigInteger, IObsData> idMap = new ConcurrentHashMap<>();
    ConcurrentNavigableMap<ObsKey, IObsData> map = new ConcurrentSkipListMap<>(new ObsKeyComparator());
    InMemoryDataStreamStore dsStore;
    IProcedureDescStore procStore;
    IFoiStore foiStore;
    AtomicLong obsCounter = new AtomicLong();
    
    
    private static class ObsKey
    {    
        BigInteger obsID;
        long dataStreamID = 0;
        FeatureId foiID = IObsData.NO_FOI;
        //Instant resultTime = null;
        Instant phenomenonTime = null;
        
        ObsKey(long dataStreamID, FeatureId foiID)
        {
            this.dataStreamID = dataStreamID;
            this.foiID = foiID;
        }
    }
    
    
    private static class ObsKeyComparator implements Comparator<ObsKey>
    {
        @Override
        public int compare(ObsKey k1, ObsKey k2)
        {
            // first compare data stream IDs
            int comp = Long.compare(k1.dataStreamID, k2.dataStreamID);
            if (comp != 0)
                return comp;
            
            // then compare foi IDs
            comp = Long.compare(k1.foiID.getInternalID(), k2.foiID.getInternalID());
            if (comp != 0)
                return comp;
            
            // don't compare result times
            // always return 0 so we store only the latest result!
            return 0;
        }        
    }
    
    
    public InMemoryObsStore(IProcedureDescStore procStore, IFoiStore foiStore)
    {
        this.procStore = procStore;
        this.foiStore = foiStore;
        this.dsStore = new InMemoryDataStreamStore(this);
    }
    
    
    Stream<Entry<BigInteger, IObsData>> getObsByDataStream(long dataStreamID, Range<Instant> phenomenonTimeRange)
    {
        ObsKey fromKey = new ObsKey(dataStreamID, new FeatureId(0, ""));        
        ObsKey toKey = new ObsKey(dataStreamID, new FeatureId(Long.MAX_VALUE, ""));
            
        return map.subMap(fromKey, toKey).entrySet().stream()
            .filter(e -> phenomenonTimeRange.contains(e.getKey().phenomenonTime))
            .map(e -> toBigIntEntry(e));
    }
    
    
    Stream<Entry<BigInteger, IObsData>> getObsByFoi(long foiID, Range<Instant> phenomenonTimeRange)
    {
        return map.entrySet().stream()
            .filter(e -> e.getValue().getFoiID().getInternalID() == foiID)
            .filter(e -> phenomenonTimeRange.contains(e.getKey().phenomenonTime))
            .map(e -> toBigIntEntry(e));
    }
    
    
    Entry<BigInteger, IObsData> toBigIntEntry(Entry<ObsKey, IObsData> e)
    {
        return new AbstractMap.SimpleEntry<>(e.getKey().obsID, e.getValue());
    }


    @Override
    public Stream<Entry<BigInteger, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields)
    {
        Stream<Entry<BigInteger, IObsData>> resultStream = null;
        
        // get phenomenon time filter
        final RangeFilter<Instant> phenomenonTimeFilter;
        if (filter.getPhenomenonTime() != null)
            phenomenonTimeFilter = filter.getPhenomenonTime();
        else
            phenomenonTimeFilter = ALL_TIMES_FILTER;
        
        // add filter on data stream
        if (filter.getFoiFilter() == null) // no FOI filter set
        {
            if (filter.getDataStreamFilter() != null)
            {
                // stream directly from list of selected datastreams
                resultStream = dsStore.selectKeys(filter.getDataStreamFilter())
                    .flatMap(id -> {
                        return getObsByDataStream(id, phenomenonTimeFilter.getRange());
                    });
            }
            else
            {
                // if no datastream or FOI selected, scan all obs
                resultStream = map.entrySet().stream()
                    .map(e -> toBigIntEntry(e));
            }
        }
        else if (filter.getDataStreamFilter() == null) // no datastream filter set
        {
            if (filter.getFoiFilter() != null)
            {
                // stream directly from list of selected fois
                resultStream = foiStore.selectKeys(filter.getFoiFilter())
                    .flatMap(id -> {
                        return getObsByFoi(id.getInternalID(), phenomenonTimeFilter.getRange());
                    });
            }
        }
        else // both datastream and FOI filters are set
        {
            // create set of selected datastreams
            Set<Long> dataStreamIDs = dsStore.selectKeys(filter.getDataStreamFilter())
                .collect(Collectors.toSet());

            if (dataStreamIDs.isEmpty())
                return Stream.empty();
            
            // stream from fois and filter on datastream IDs
            resultStream = foiStore.selectKeys(filter.getFoiFilter())
                .flatMap(id -> {
                    return getObsByFoi(id.getInternalID(), phenomenonTimeFilter.getRange())
                        .filter(e -> dataStreamIDs.contains(e.getValue().getDataStreamID()));
                });
        }
            
        // filter with predicate and apply limit
        return resultStream
            .filter(e -> filter.test(e.getValue()))
            .limit(filter.getLimit());
    }


    @Override
    public void clear()
    {
        idMap.clear();
        map.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        return key instanceof BigInteger && idMap.containsKey(key);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return map.containsValue(val);
    }


    public Set<Entry<BigInteger, IObsData>> entrySet()
    {
        return new AbstractSet<Entry<BigInteger, IObsData>>()
        {
            @Override
            public Iterator<Entry<BigInteger, IObsData>> iterator()
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public int size()
            {
                return map.size();
            }            
        };
    }


    @Override
    public IObsData get(Object key)
    {
        if (!(key instanceof BigInteger))
            return null;
        
        return idMap.get(key);
    }


    @Override
    public IObsData put(BigInteger key, IObsData obs)
    {
        ObsKey obsKey = new ObsKey(obs.getDataStreamID(), obs.getFoiID());
        obsKey.obsID = key;
        obsKey.phenomenonTime = obs.getPhenomenonTime();
        
        map.put(obsKey, obs);
        return idMap.put(key, obs);
    }


    @Override
    public BigInteger add(IObsData obs)
    {
        ObsKey key = new ObsKey(obs.getDataStreamID(), obs.getFoiID());
        key.obsID = BigInteger.valueOf(obsCounter.incrementAndGet());
        key.phenomenonTime = obs.getPhenomenonTime();            
                
        map.put(key, obs);
        idMap.put(key.obsID, obs);
        return key.obsID;
    }


    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }


    @Override
    public Set<BigInteger> keySet()
    {
        return Collections.unmodifiableSet(idMap.keySet());
    }


    @Override
    public int size()
    {
        return map.size();
    }


    @Override
    public Collection<IObsData> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }


    public boolean remove(Object key, Object val)
    {
        return map.remove(key, val);
    }


    @Override
    public IObsData remove(Object key)
    {
        IObsData oldObs = idMap.remove(key);
        
        ObsKey obsKey = new ObsKey(oldObs.getDataStreamID(), oldObs.getFoiID());
        obsKey.obsID = BigInteger.valueOf(obsCounter.incrementAndGet());
        obsKey.phenomenonTime = oldObs.getPhenomenonTime(); 
        
        map.remove(obsKey);
        return oldObs;
    }


    @Override
    public long getNumRecords()
    {
        return idMap.size();
    }


    @Override
    public IDataStreamStore getDataStreams()
    {
        return dsStore;
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
