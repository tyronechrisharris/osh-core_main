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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.DataStreamInfo;
import org.sensorhub.api.datastore.IDataStreamInfo;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.vast.util.Asserts;


/**
 * <p>
 * In-memory implementation of a datastream store backed by a {@link java.util.NavigableMap}.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryDataStreamStore extends InMemoryDataStore implements IDataStreamStore
{
    ConcurrentNavigableMap<Long, IDataStreamInfo> map = new ConcurrentSkipListMap<>();
    InMemoryObsStore obsStore;
    
    
    public InMemoryDataStreamStore(InMemoryObsStore obsStore)
    {
        this.obsStore = obsStore;
    }


    @Override
    public synchronized Long add(IDataStreamInfo dsInfo)
    {
        Asserts.checkNotNull(dsInfo, DataStreamInfo.class);
        
        Long nextId = map.lastKey()+1;
        map.put(nextId, dsInfo);
        return nextId;
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }


    @Override
    public Stream<Entry<Long, IDataStreamInfo>> selectEntries(DataStreamFilter query, Set<DataStreamInfoField> fields)
    {
        Stream<Entry<Long, IDataStreamInfo>> resultStream;
                
        if (query.getInternalIDs() != null)
        {
            resultStream = query.getInternalIDs().stream()
                .map(id -> {
                    IDataStreamInfo val = map.get(id);
                    return new AbstractMap.SimpleEntry<>(id, val);
                });
        }
        else
            resultStream = map.entrySet().stream();
        
        // also filter on selected procedures
        if (query.getProcedureFilter() != null)
        {
            Set<Long> selectedProcedures = obsStore.procStore.selectKeys(query.getProcedureFilter())
                .map(k -> k.getInternalID())
                .collect(Collectors.toSet());
            
            resultStream = resultStream.filter(e -> selectedProcedures.contains(e.getValue().getProcedure().getInternalID()));
        }
        
        // filter with predicate and apply limit
        return resultStream
            .filter(e -> query.test(e.getValue()))
            .limit(query.getLimit());
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


    public Set<Entry<Long, IDataStreamInfo>> entrySet()
    {
        return map.entrySet();
    }


    @Override
    public IDataStreamInfo get(Object key)
    {
        return map.get(key);
    }


    @Override
    public IDataStreamInfo put(Long key, IDataStreamInfo value)
    {
        return map.put(key, value);
    }


    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }


    @Override
    public Set<Long> keySet()
    {
        return Collections.unmodifiableSet(map.keySet());
    }


    @Override
    public int size()
    {
        return map.size();
    }


    @Override
    public Collection<IDataStreamInfo> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }


    public boolean remove(Object key, Object val)
    {
        return map.remove(key, val);
    }


    @Override
    public IDataStreamInfo remove(Object key)
    {
        return map.remove(key);
    }
}
