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

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.ObsKey;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.ObsData;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.api.datastore.ObsStats;
import org.sensorhub.api.datastore.ObsStatsQuery;
import org.sensorhub.api.datastore.RangeFilter;
import org.sensorhub.api.procedure.IProcedureDescriptionStore;
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
    
    ConcurrentNavigableMap<ObsKey, ObsData> map = new ConcurrentSkipListMap<>(new ObsKeyComparator());
    InMemoryDataStreamStore dsStore;
    IProcedureDescriptionStore procStore;
    IFoiStore foiStore;
    
    
    static class ObsKeyComparator implements Comparator<ObsKey>
    {
        @Override
        public int compare(ObsKey k1, ObsKey k2)
        {
            // first compare data stream IDs
            int comp = Long.compare(k1.getDataStreamID(), k2.getDataStreamID());
            if (comp != 0)
                return comp;
            
            // then compare foi IDs
            comp = Long.compare(k1.getFoiID().getInternalID(), k2.getFoiID().getInternalID());
            if (comp != 0)
                return comp;
            
            // don't compare result times
            // always return 0 so we store only the latest result!
            return 0;
        }        
    }
    
    
    public InMemoryObsStore(IProcedureDescriptionStore procStore, IFoiStore foiStore)
    {
        this.procStore = procStore;
        this.foiStore = foiStore;
        this.dsStore = new InMemoryDataStreamStore(this);
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }
    
    
    Stream<Entry<ObsKey, ObsData>> getObsByDataStream(long dataStreamID, Range<Instant> phenomenonTimeRange)
    {
        ObsKey fromKey = new ObsKey(dataStreamID, new FeatureId(0), null);        
        ObsKey toKey = new ObsKey(dataStreamID, new FeatureId(Long.MAX_VALUE), null);
            
        return map.subMap(fromKey, toKey).entrySet().stream()
            .filter(e -> phenomenonTimeRange.contains(e.getKey().getPhenomenonTime()));
    }
    
    
    Stream<Entry<ObsKey, ObsData>> getObsByFoi(long foiID, Range<Instant> phenomenonTimeRange)
    {
        return map.entrySet().stream()
            .filter(e -> e.getKey().getFoiID().getInternalID() == foiID)
            .filter(e -> phenomenonTimeRange.contains(e.getKey().getPhenomenonTime()));
    }


    @Override
    public Stream<Entry<ObsKey, ObsData>> selectEntries(ObsFilter filter)
    {
        Stream<Entry<ObsKey, ObsData>> resultStream = null;
        
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
                resultStream = map.entrySet().stream();
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
                        .filter(e -> dataStreamIDs.contains(e.getKey().getDataStreamID()));
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
        return map.containsKey(key);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return map.containsValue(val);
    }


    public Set<Entry<ObsKey, ObsData>> entrySet()
    {
        return map.entrySet();
    }


    @Override
    public ObsData get(Object key)
    {
        return map.get(key);
    }


    @Override
    public ObsData put(ObsKey key, ObsData value)
    {
        return map.put(key, value);
    }


    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }


    @Override
    public Set<ObsKey> keySet()
    {
        return Collections.unmodifiableSet(map.keySet());
    }


    @Override
    public int size()
    {
        return map.size();
    }


    @Override
    public Collection<ObsData> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }


    public boolean remove(Object key, Object val)
    {
        return map.remove(key, val);
    }


    @Override
    public ObsData remove(Object key)
    {
        return map.remove(key);
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
