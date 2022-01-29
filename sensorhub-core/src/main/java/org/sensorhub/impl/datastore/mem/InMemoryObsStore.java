/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUObsData WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.mem;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStats;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.collect.Range;


/**
 * <p>
 * In-memory implementation of an observation store backed by a {@link NavigableMap}.
 * This implementation is only used to store the latest system state and
 * thus only stores the latest observation of each data stream and FOI
 * combination.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryObsStore extends InMemoryDataStore implements IObsStore
{
    static final TemporalFilter ALL_TIMES_FILTER = new TemporalFilter.Builder().withAllTimes().build();
    
    NavigableMap<ObsKey, IObsData> map = new ConcurrentSkipListMap<>(new ObsKeyComparator());
    InMemoryDataStreamStore dataStreamStore;
    IFoiStore foiStore;
    AtomicLong obsCounter = new AtomicLong();
    
    
    private static class ObsKey
    {    
        long dataStreamID = 0;
        long foiID = 0;
        //Instant resultTime = null;
        Instant phenomenonTime = null;
        
        ObsKey(long dataStreamID, long foiID, Instant phenomenonTime)
        {
            this.dataStreamID = dataStreamID;
            this.foiID = foiID;
            this.phenomenonTime = phenomenonTime;
        }

        @Override
        public String toString()
        {
            return ObjectUtils.toString(this, true);
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
            comp = Long.compare(k1.foiID, k2.foiID);
            if (comp != 0)
                return comp;
            
            // then compare phenomenon times
            return k1.phenomenonTime.compareTo(k2.phenomenonTime);
        }
    }
    
    
    public InMemoryObsStore()
    {
        this.dataStreamStore = new InMemoryDataStreamStore(this);
    }
    
    
    BigInteger toExternalKey(ObsKey obsKey)
    {
        // compute internal ID
        ByteBuffer buf = ByteBuffer.allocate(28);
        buf.putLong(obsKey.dataStreamID);
        buf.putLong(obsKey.foiID);
        buf.putLong(obsKey.phenomenonTime.getEpochSecond());
        buf.putInt(obsKey.phenomenonTime.getNano());
        return new BigInteger(buf.array(), 0, buf.position());
    }
    
    
    ObsKey toInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger);
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            byte[] bigIntBytes = key.toByteArray();
            ByteBuffer buf = ByteBuffer.allocate(28);
            for (int i=0; i<(28-bigIntBytes.length); i++)
                buf.put((byte)0);
            buf.put(bigIntBytes);
            buf.flip();
            
            long dsID = buf.getLong();
            long foiID = buf.getLong();
            Instant phenomenonTime = Instant.ofEpochSecond(buf.getLong(), buf.getInt());
            return new ObsKey(dsID, foiID, phenomenonTime);
        }
        catch (Exception e)
        {
            // invalid bigint key// return key object that will never match
            return new ObsKey(0, 0, Instant.MAX);
        }
    }
    
    
    Stream<Entry<ObsKey, IObsData>> getObsByDataStream(long dataStreamID)
    {
        ObsKey fromKey = new ObsKey(dataStreamID, 0, Instant.MIN);
        ObsKey toKey = new ObsKey(dataStreamID, Long.MAX_VALUE, Instant.MAX);
        
        return map.subMap(fromKey, true, toKey, true).entrySet().stream();
    }
    
    
    Stream<Entry<ObsKey, IObsData>> getObsByFoi(long foiID)
    {
        return map.entrySet().stream()
            .filter(e -> e.getValue().getFoiID() == foiID);
    }
    
    
    Stream<Entry<ObsKey, IObsData>> getObsByDataStreamAndFoi(long dataStreamID, long foiID)
    {
        ObsKey fromKey = new ObsKey(dataStreamID, foiID, Instant.MIN);
        ObsKey toKey = new ObsKey(dataStreamID, foiID, Instant.MAX);
        
        return map.subMap(fromKey, true, toKey, true).entrySet().stream();
    }
    
    
    Entry<BigInteger, IObsData> toExternalEntry(Entry<ObsKey, IObsData> e)
    {
        return new AbstractMap.SimpleEntry<>(toExternalKey(e.getKey()), e.getValue());
    }


    @Override
    public Stream<Entry<BigInteger, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields)
    {
        Stream<Entry<ObsKey, IObsData>> resultStream = null;
        
        // fetch obs directly in case of filtering by internal IDs
        if (filter.getInternalIDs() != null)
        {
            resultStream = filter.getInternalIDs().stream()
                .map(k -> {
                    var obsKey = toInternalKey(k);
                    var obs = map.get(obsKey);
                    return obs != null ?
                        (Entry<ObsKey, IObsData>)new AbstractMap.SimpleEntry<>(obsKey, obs) :
                        null;
                })
                .filter(Objects::nonNull);
        }
        
        // if no datastream nor FOI filter used, scan all obs
        else if (filter.getDataStreamFilter() == null && filter.getFoiFilter() == null)
        {
            resultStream = map.entrySet().stream();
        }
        
        // only datastream filter used
        else if (filter.getDataStreamFilter() != null && filter.getFoiFilter() == null)
        {
            // stream directly from list of selected datastreams
            resultStream = DataStoreUtils.selectDataStreamIDs(dataStreamStore, filter.getDataStreamFilter())
                .flatMap(id -> getObsByDataStream(id));
        }
        
        // only FOI filter used
        else if (filter.getFoiFilter() != null && filter.getDataStreamFilter() == null)
        {
            // stream directly from list of selected fois
            resultStream = DataStoreUtils.selectFeatureIDs(foiStore, filter.getFoiFilter())
                .flatMap(id -> getObsByFoi(id));
        }
        
        // both datastream and FOI filters used
        else
        {
            // create set of selected datastreams
            Set<Long> dataStreamIDs = DataStoreUtils.selectDataStreamIDs(dataStreamStore, filter.getDataStreamFilter())
                .collect(Collectors.toSet());
            if (dataStreamIDs.isEmpty())
                return Stream.empty();
            
            // cross product between list of datastream IDs and foiIDs
            resultStream = dataStreamIDs.stream()
                .flatMap(dsID -> {
                    return DataStoreUtils.selectFeatureIDs(foiStore, filter.getFoiFilter())
                        .flatMap(foiID -> {
                            return getObsByDataStreamAndFoi(dsID, foiID);
                        });
                });
        }
            
        // filter with predicate and apply limit
        return resultStream
            .filter(e -> filter.test(e.getValue()))
            .limit(filter.getLimit())
            .map(e -> toExternalEntry(e));
    }


    @Override
    public void clear()
    {
        map.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        return key instanceof BigInteger && map.containsKey(toInternalKey(key));
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
        var k = toInternalKey(key);
        return map.get(k);
    }


    @Override
    public IObsData put(BigInteger key, IObsData obs)
    {
        ObsKey obsKey = toInternalKey(key);
        IObsData oldObs = map.replace(obsKey, obs);
        
        if (oldObs == null)
            throw new IllegalArgumentException("put can only be used to update existing keys");
        
        return oldObs;
    }


    @Override
    public BigInteger add(IObsData obs)
    {
        ObsKey key = new ObsKey(
            obs.getDataStreamID(),
            obs.getFoiID(),
            obs.getPhenomenonTime());
        
        // add new obs and remove older ones atomically
        map.compute(key, (k,v) -> {
            removeOlderObs(key, obs);
            return obs;
        });
        
        return toExternalKey(key);
    }
    
    
    protected void removeOlderObs(ObsKey newKey, IObsData newObs)
    {
        var first = new ObsKey(newObs.getDataStreamID(), newObs.getFoiID(), Instant.MIN);
        var last = new ObsKey(newObs.getDataStreamID(), newObs.getFoiID(), Instant.MAX);
        
        // remove all other obs for same stream/foi combination
        var it = map.subMap(first, last).keySet().iterator();
        while (it.hasNext())
        {
            var oldKey = it.next();
            if (oldKey != newKey)
                it.remove();
        }
    }


    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }


    @Override
    public Set<BigInteger> keySet()
    {
        return map.keySet().stream()
            .map(this::toExternalKey)
            .collect(Collectors.toSet());
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
        return map.remove(toInternalKey(key));
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }


    @Override
    public IDataStreamStore getDataStreams()
    {
        return dataStreamStore;
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        var filter = query.getObsFilter() != null ? query.getObsFilter() : this.selectAllFilter();
        
        // simple implementation since we know we have only one obs per datastream/foi pair
        return select(filter)
            .map(obs -> {
                var foiID = obs.getFoiID() > 0 ?
                    new FeatureId(obs.getFoiID(), "urn:foi:unknown") :
                    FeatureId.NULL_FEATURE;
                
                var stats = new ObsStats.Builder()
                    .withDataStreamID(obs.getDataStreamID())
                    .withFoiID(foiID)
                    .withPhenomenonTimeRange(TimeExtent.instant(obs.getPhenomenonTime()))
                    .withResultTimeRange(TimeExtent.instant(obs.getResultTime()))
                    .withTotalObsCount(1);
                
                // compute histogram if requested
                if (query.getHistogramBinSize() != null)
                {
                    var histogramTimeRange = filter.getPhenomenonTime().getRange();
                    if (histogramTimeRange.lowerEndpoint() == Instant.MIN || histogramTimeRange.upperEndpoint() == Instant.MAX)
                    {
                        var truncated = obs.getPhenomenonTime().truncatedTo(ChronoUnit.MINUTES);
                        var begin = truncated.minus(5, ChronoUnit.MINUTES);
                        var end = begin.plus(5, ChronoUnit.MINUTES);
                        histogramTimeRange = Range.closed(begin, end);
                    }
                    
                    long obsTime = obs.getPhenomenonTime().getEpochSecond();
                    long start = histogramTimeRange.lowerEndpoint().getEpochSecond();
                    long end = histogramTimeRange.upperEndpoint().getEpochSecond();
                    long dt = query.getHistogramBinSize().getSeconds();
                    int numBins = (int)Math.ceil((double)(end - start)/dt);
                    int[] counts = new int[numBins];
                    for (int i = 0; i < numBins; i++)
                    {
                        long t = start + i*dt;
                        if (obsTime >= t && obsTime < t+dt)
                        {
                            counts[i] = 1;
                            break;
                        }
                    }
                    
                    stats.withObsCountByTime(counts);
                }
                
                return stats.build();
            });
    }
    
    
    @Override
    public void linkTo(IFoiStore foiStore)
    {
        this.foiStore = Asserts.checkNotNull(foiStore, IFoiStore.class);
    }
}
