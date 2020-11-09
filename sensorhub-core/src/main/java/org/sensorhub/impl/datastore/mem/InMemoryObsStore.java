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
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsStats;
import org.vast.util.Asserts;


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
    static final TemporalFilter ALL_TIMES_FILTER = new TemporalFilter.Builder().withAllTimes().build();
    
    ConcurrentNavigableMap<ObsKey, IObsData> map = new ConcurrentSkipListMap<>(new ObsKeyComparator());
    InMemoryDataStreamStore dsStore;
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
            
            // don't compare result times
            // always return 0 so we store only the latest result!
            return 0;
        }        
    }
    
    
    public InMemoryObsStore()
    {
        this.dsStore = new InMemoryDataStreamStore(this);
    }
    
    
    BigInteger toPublicKey(ObsKey obsKey)
    {
        // compute internal ID
        ByteBuffer buf = ByteBuffer.allocate(24);
        buf.putInt((int)obsKey.dataStreamID);
        buf.putInt((int)obsKey.foiID);
        buf.putInt((int)(obsKey.phenomenonTime.getEpochSecond()));
        buf.putInt((int)(obsKey.phenomenonTime.getNano()));
        return new BigInteger(buf.array(), 0, buf.position());
    }
    
    
    ObsKey toInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger);
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            ByteBuffer buf = ByteBuffer.wrap(key.toByteArray());
            long dsID = buf.getInt();
            long foiID = buf.getInt();
            Instant phenomenonTime = Instant.ofEpochSecond(buf.getInt(), buf.getInt());
            return new ObsKey(dsID, foiID, phenomenonTime);
        }
        catch (Exception e)
        {
            // invalid bigint key
            return null;
        }
    }
    
    
    Stream<Entry<BigInteger, IObsData>> getObsByDataStream(long dataStreamID, TemporalFilter phenomenonTimeFilter)
    {
        ObsKey fromKey = new ObsKey(dataStreamID, 0, null);        
        ObsKey toKey = new ObsKey(dataStreamID, Long.MAX_VALUE, null);
        
        return map.subMap(fromKey, toKey).entrySet().stream()
            .filter(e -> phenomenonTimeFilter.isLatestTime() || phenomenonTimeFilter.test(e.getKey().phenomenonTime))
            .map(e -> toBigIntEntry(e));
    }
    
    
    Stream<Entry<BigInteger, IObsData>> getObsByFoi(long foiID, TemporalFilter phenomenonTimeFilter)
    {
        return map.entrySet().stream()
            .filter(e -> e.getValue().getFoiID().getInternalID() == foiID)
            .filter(e -> phenomenonTimeFilter.isLatestTime() || phenomenonTimeFilter.test(e.getKey().phenomenonTime))
            .map(e -> toBigIntEntry(e));
    }
    
    
    Entry<BigInteger, IObsData> toBigIntEntry(Entry<ObsKey, IObsData> e)
    {
        return new AbstractMap.SimpleEntry<>(toPublicKey(e.getKey()), e.getValue());
    }


    @Override
    public Stream<Entry<BigInteger, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields)
    {
        Stream<Entry<BigInteger, IObsData>> resultStream = null;
        
        // get phenomenon time filter
        final TemporalFilter phenomenonTimeFilter;
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
                    .flatMap(k -> {
                        return getObsByDataStream(k.getInternalID(), phenomenonTimeFilter);
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
                        return getObsByFoi(id.getInternalID(), phenomenonTimeFilter);
                    });
            }
        }
        else // both datastream and FOI filters are set
        {
            // create set of selected datastreams
            Set<Long> dataStreamIDs = dsStore.selectKeys(filter.getDataStreamFilter())
                .map(k -> k.getInternalID())
                .collect(Collectors.toSet());

            if (dataStreamIDs.isEmpty())
                return Stream.empty();
            
            // stream from fois and filter on datastream IDs
            resultStream = foiStore.selectKeys(filter.getFoiFilter())
                .flatMap(id -> {
                    return getObsByFoi(id.getInternalID(), phenomenonTimeFilter)
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
        return map.get(toInternalKey(key));
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
            obs.getFoiID().getInternalID(),
            obs.getPhenomenonTime());
        map.remove(key);
        map.put(key, obs);
        return toPublicKey(key);
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
            .map(this::toPublicKey)
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
        return dsStore;
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
        Asserts.checkNotNull(foiStore, IFoiStore.class);
        
        if (this.foiStore != foiStore)
        {
            this.foiStore = foiStore;
            foiStore.linkTo(this);
        }
    }
}
