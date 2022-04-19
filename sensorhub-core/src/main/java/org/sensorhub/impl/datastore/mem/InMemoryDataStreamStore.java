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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.obs.DataStreamInfoWrapper;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;


/**
 * <p>
 * In-memory implementation of a datastream store backed by a {@link NavigableMap}.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryDataStreamStore implements IDataStreamStore
{
    final NavigableMap<DataStreamKey, IDataStreamInfo> map = new ConcurrentSkipListMap<>();
    final NavigableMap<BigId, Set<DataStreamKey>> procIdToDsKeys = new ConcurrentSkipListMap<>();
    final InMemoryObsStore obsStore;
    ISystemDescStore systemStore;
    
    
    class DataStreamInfoWithTimeRanges extends DataStreamInfoWrapper
    {
        BigId id;
        TimeExtent phenomenonTimeRange;
        
        DataStreamInfoWithTimeRanges(BigId id, IDataStreamInfo dsInfo)
        {
            super(dsInfo);
            this.id = id;
        }
        
        @Override
        public TimeExtent getPhenomenonTimeRange()
        {
            if (phenomenonTimeRange == null)
            {
                var obsIt = obsStore.select(new ObsFilter.Builder().withDataStreams(id).build()).iterator();
                
                Instant begin = Instant.MAX;
                Instant end = Instant.MIN;
                while (obsIt.hasNext())
                {
                    var t = obsIt.next().getPhenomenonTime();
                    if (t.isBefore(begin))
                        begin = t;
                    if (t.isAfter(end))
                        end = t;
                }
                
                if (begin == Instant.MAX || end == Instant.MIN)
                    phenomenonTimeRange = null;
                else
                    phenomenonTimeRange = TimeExtent.period(begin, end);
            }
            
            return phenomenonTimeRange;
        }
    }


    public InMemoryDataStreamStore(InMemoryObsStore obsStore)
    {
        this.obsStore = Asserts.checkNotNull(obsStore, IObsStore.class);
    }
    
    
    @Override
    public synchronized DataStreamKey add(IDataStreamInfo dsInfo) throws DataStoreException
    {
        DataStoreUtils.checkDataStreamInfo(systemStore, dsInfo);
        
        // use valid time of parent system or current time if none was set
        dsInfo = DataStoreUtils.ensureValidTime(systemStore, dsInfo);

        // create key
        var newKey = generateKey(dsInfo);

        // add to store
        put(newKey, dsInfo, false);
        return newKey;
    }
    
    
    protected DataStreamKey generateKey(IDataStreamInfo dsInfo)
    {
        //long internalID = map.isEmpty() ? 1 : map.lastKey().getInternalID()+1;
        //return new DataStreamKey(internalID);
        
        // make sure that the same system/output combination always returns the same ID
        // this will keep things more consistent across restart
        var hash = Objects.hash(
            dsInfo.getSystemID().getInternalID(),
            dsInfo.getOutputName(),
            dsInfo.getValidTime());
        return new DataStreamKey(obsStore.idScope, hash & 0xFFFFFFFFL);
    }


    @Override
    public IDataStreamInfo get(Object key)
    {
        var dsKey = DataStoreUtils.checkDataStreamKey(key);
        
        var val = map.get(dsKey);
        if (val != null)
            return new DataStreamInfoWithTimeRanges(dsKey.getInternalID(), val);
        else
            return null;
    }


    @Override
    public Stream<Entry<DataStreamKey, IDataStreamInfo>> selectEntries(DataStreamFilter filter, Set<DataStreamInfoField> fields)
    {
        Stream<DataStreamKey> keyStream = null;
        Stream<Entry<DataStreamKey, IDataStreamInfo>> resultStream;

        if (filter.getInternalIDs() != null)
        {
            keyStream = filter.getInternalIDs().stream()
                .map(id -> new DataStreamKey(id));
        }
        
        // or filter on selected systems
        else if (filter.getSystemFilter() != null)
        {
            keyStream = DataStoreUtils.selectSystemIDs(systemStore, filter.getSystemFilter()) 
                .flatMap(procId -> {
                    var dsKeys = procIdToDsKeys.get(procId);
                    return dsKeys != null ? dsKeys.stream() : Stream.empty();
                });
        }        
        
        if (keyStream != null)
        {
            resultStream = keyStream.map(key -> {
                var dsInfo = map.get(key);
                if (dsInfo == null)
                    return null;
                return (Entry<DataStreamKey, IDataStreamInfo>)new AbstractMap.SimpleEntry<>(key, dsInfo);
            })
            .filter(Objects::nonNull);
        }
        else
        {
            // stream all entries
            resultStream = map.entrySet().stream();
        }
        
        // filter with predicate, apply limit and wrap with DataStreamInfoWithTimeRanges
        return resultStream
            .filter(e -> filter.test(e.getValue()))
            .limit(filter.getLimit()).map(e -> {
                IDataStreamInfo val = new DataStreamInfoWithTimeRanges(e.getKey().getInternalID(), e.getValue());
                return (Entry<DataStreamKey, IDataStreamInfo>)new AbstractMap.SimpleEntry<>(e.getKey(), val);
            });
    }


    @Override
    public IDataStreamInfo put(DataStreamKey key, IDataStreamInfo dsInfo)
    {
        DataStoreUtils.checkDataStreamKey(key);
        
        try
        {
            DataStoreUtils.checkDataStreamInfo(systemStore, dsInfo);
            return put(key, dsInfo, true);
        }
        catch (DataStoreException e)
        {
            throw new IllegalArgumentException(e);
        }
    }
    
    
    protected synchronized IDataStreamInfo put(DataStreamKey dsKey, IDataStreamInfo dsInfo, boolean replace) throws DataStoreException
    {
        // if needed, add a new datastream keyset for the specified system
        var procDsKeys = procIdToDsKeys.compute(dsInfo.getSystemID().getInternalID(), (id, keys) -> {
            if (keys == null)
                keys = new ConcurrentSkipListSet<>();
            return keys;
        });
        
        // scan existing datastreams associated to the same system
        for (var key: procDsKeys)
        {
            var prevDsInfo = map.get(key);
            
            if (prevDsInfo != null &&
                prevDsInfo.getSystemID().getInternalID() == dsInfo.getSystemID().getInternalID() &&
                prevDsInfo.getOutputName().equals(dsInfo.getOutputName()))
            {    
                var prevValidTime = prevDsInfo.getValidTime().begin();
                var newValidTime = dsInfo.getValidTime().begin();
                
                // error if datastream with same system/name/validTime already exists
                if (prevValidTime.equals(newValidTime))
                    throw new DataStoreException(DataStoreUtils.ERROR_EXISTING_DATASTREAM);
                
                // don't add if previous entry had a more recent valid time
                // or if new entry is dated in the future
                if (prevValidTime.isAfter(newValidTime) || newValidTime.isAfter(Instant.now()))
                    return prevDsInfo;
                
                // otherwise remove existing datastream and associated observations
                map.remove(key);
                obsStore.removeEntries(new ObsFilter.Builder()
                    .withDataStreams(key.getInternalID())
                    .build());
                break;
            }
        }
        
        // add new datastream
        var oldDsInfo = map.put(dsKey, dsInfo);
        procDsKeys.add(dsKey);
        return oldDsInfo;
    }


    @Override
    public IDataStreamInfo remove(Object key)
    {
        var dsKey = DataStoreUtils.checkDataStreamKey(key);
        var oldValue = new AtomicReference<IDataStreamInfo>();
        
        map.computeIfPresent(dsKey, (k, v) -> {
            
            // remove all associated obs
            obsStore.removeEntries(new ObsFilter.Builder()
                .withDataStreams(dsKey.getInternalID())
                .build());
            
            // remove from secondary index
            procIdToDsKeys.get(v.getSystemID().getInternalID()).remove(dsKey);
            
            // remove entry
            oldValue.set(v);
            return null; 
        });
        
        return oldValue.get();
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }


    @Override
    public void clear()
    {
        map.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        var dsKey = DataStoreUtils.checkDataStreamKey(key);
        return map.containsKey(dsKey);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return map.containsValue(val);
    }


    @Override
    public Set<Entry<DataStreamKey, IDataStreamInfo>> entrySet()
    {
        return map.entrySet();
    }


    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }


    @Override
    public Set<DataStreamKey> keySet()
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


    @Override
    public String getDatastoreName()
    {
        return getClass().getSimpleName();
    }


    @Override
    public void commit()
    {        
    }


    @Override
    public void backup(OutputStream is) throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void restore(InputStream os) throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isReadOnly()
    {
        return false;
    }
    
    
    @Override
    public void linkTo(ISystemDescStore systemStore)
    {
        this.systemStore = Asserts.checkNotNull(systemStore, ISystemDescStore.class);
    }
}
