/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVVarLongDataType;
import org.h2.mvstore.RangeCursor;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.DataStreamInfo;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.api.datastore.ProcedureFilter;
import org.sensorhub.impl.datastore.h2.H2Utils.Holder;
import org.vast.util.Asserts;
import com.google.common.base.Strings;
import com.google.common.collect.Range;


/**
 * <p>
 * Datastream Store implementation based on H2 MVStore.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 19, 2019
 */
class MVDataStreamStoreImpl implements IDataStreamStore
{
    private static final String DATASTREAM_MAP_NAME = "@dstreams";
    private static final String DATASTREAM_PROC_MAP_NAME = "@dstreams_proc";
    
    MVStore mvStore;
    MVObsStoreImpl obsStore;
    MVBTreeMap<Long, DataStreamInfo> dataStreamIndex;
    MVBTreeMap<MVDataStreamProcKey, Boolean> dataStreamByProcIndex;
    IdProvider idProvider;
    
    
    MVDataStreamStoreImpl(MVObsStoreImpl obsStore, IdProvider idProvider)
    {
        this.obsStore = Asserts.checkNotNull(obsStore, MVObsStoreImpl.class);
        this.mvStore = Asserts.checkNotNull(obsStore.mvStore, MVStore.class);
                        
        // open observation map
        String mapName = DATASTREAM_MAP_NAME + ":" + obsStore.getDatastoreName();
        this.dataStreamIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<Long, DataStreamInfo>()
                .keyType(new MVVarLongDataType())
                .valueType(new MVDataStreamInfoDataType()));
        
        // open observation series map
        mapName = DATASTREAM_PROC_MAP_NAME + ":" + obsStore.getDatastoreName();
        this.dataStreamByProcIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVDataStreamProcKey, Boolean>()
                .keyType(new MVDataStreamProcKeyDataType())
                .valueType(new MVVoidDataType()));
        
        // Id provider
        this.idProvider = idProvider;
        if (idProvider == null) // use default if nothing is set
        {
            this.idProvider = () -> {
                if (dataStreamIndex.isEmpty())
                    return 1;
                else
                    return dataStreamIndex.lastKey()+1;
            };
        }
    }
    
    
    @Override
    public String getDatastoreName()
    {
        return obsStore.getDatastoreName();
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        return obsStore.getTimeZone();
    }


    @Override
    public long getNumRecords()
    {
        return dataStreamIndex.sizeAsLong();
    }
    
    
    @Override
    public synchronized Long add(DataStreamInfo dsInfo)
    {
        Asserts.checkNotNull(dsInfo, DataStreamInfo.class);
        Asserts.checkNotNull(dsInfo.getProcedure(), "procedureID");
        Asserts.checkNotNull(dsInfo.getOutputName(), "outputName");
        
        MVDataStreamProcKey procKey = new MVDataStreamProcKey(
            dsInfo.getProcedure().getInternalID(),
            dsInfo.getOutputName(),
            dsInfo.getRecordVersion());
        Asserts.checkArgument(!dataStreamByProcIndex.containsKey(procKey), "A datastream with the same name already exists");            
        
        // generate key
        Long internalID = idProvider.newInternalID();
        
        // add to store
        DataStreamInfo oldValue = put(internalID, dsInfo);
        Asserts.checkState(oldValue == null, "Duplicate key");
            
        return internalID;
    }


    public Stream<Long> selectProcedureIDs(ProcedureFilter filter)
    {
        if (filter.getInternalIDs() != null &&
            filter.getInternalIDs().isSet() &&
            filter.getLocationFilter() == null )
        {
            // if only internal IDs were specified, no need to search the feature store
            return filter.getInternalIDs().getSet().stream();
        }
        else
        {
            // otherwise get all feature keys matching the filter from linked feature store
            // we apply the distinct operation to make sure the same feature is not
            // listed twice (it can happen when there exists several versions of the 
            // same feature with different valid times)
            return obsStore.procedureStore.selectKeys(filter)
                .map(k -> k.getInternalID())
                .distinct();
        }
    }
    
    
    Stream<Long> getDataStreamIdsByProcedure(long procID, Set<String> outputNames, Range<Integer> versionRange)
    {
        MVDataStreamProcKey first = new MVDataStreamProcKey(procID, "", 0);
        RangeCursor<MVDataStreamProcKey, Boolean> cursor = new RangeCursor<>(dataStreamByProcIndex, first);
        
        Stream<MVDataStreamProcKey> keyStream = cursor.keyStream()
            .takeWhile(k -> k.procedureID == procID);
        
        // we post filter output names and versions during the scan
        // since number of outputs and versions is usually small, this should
        // be faster than looking up each output separately
        return postFilterKeyStream(keyStream, outputNames, versionRange)
            .map(k -> k.internalID);
    }
    
    
    Stream<Long> getDataStreamIdsFromAllProcedures(Set<String> outputNames, Range<Integer> versionRange)
    {
        Stream<MVDataStreamProcKey> keyStream = dataStreamByProcIndex.keySet().stream();
        
        // yikes we're doing a full index scan here!
        return postFilterKeyStream(keyStream, outputNames, versionRange)
            .map(k -> k.internalID);
    }
    
    
    Stream<MVDataStreamProcKey> postFilterKeyStream(Stream<MVDataStreamProcKey> keyStream, Set<String> outputNames, Range<Integer> versionRange)
    {
        if (outputNames != null)
            keyStream = keyStream.filter(k -> outputNames.contains(k.outputName));
        
        if (versionRange != null)
        {
            if (DataStreamFilter.LAST_VERSION.equals(versionRange))
            {
                Holder<MVDataStreamProcKey> lastKey = new Holder<>();
                keyStream = keyStream.map(k -> {
                    MVDataStreamProcKey saveLastKey = lastKey.value;
                    lastKey.value = k;
                    // keep only the last version that is the first record of
                    // each procID + output name combination
                    if (saveLastKey == null || 
                        k.procedureID != saveLastKey.procedureID ||
                        !k.outputName.equals(saveLastKey.outputName)) {
                        return k;
                    }
                    return null;
                }).filter(Objects::nonNull);
            }
            else
                keyStream = keyStream.filter(k -> versionRange.contains(k.recordVersion));
        }
        
        return keyStream;
    }
    
    
    @Override
    public Stream<Entry<Long, DataStreamInfo>> selectEntries(DataStreamFilter filter)
    {
        Stream<Entry<Long, DataStreamInfo>> resultStream;
        
        // if filtering by internal IDs, use these IDs directly
        if (filter.getInternalIDs() != null)
        {
            resultStream = filter.getInternalIDs().stream()
                .map(id -> dataStreamIndex.getEntry(id))
                .filter(Objects::nonNull);
        }
        
        // if procedure filter is used
        else if (filter.getProcedureFilter() != null)
        {
            // stream directly from list of selected procedures
            resultStream = selectProcedureIDs(filter.getProcedureFilter())
                .flatMap(id -> getDataStreamIdsByProcedure(id, filter.getOutputNames(), filter.getVersions()))
                .map(id -> dataStreamIndex.getEntry(id))
                .filter(Objects::nonNull);
        }
        
        // else filter data stream only by output name and version
        else
        {
            resultStream = getDataStreamIdsFromAllProcedures(filter.getOutputNames(), filter.getVersions())
                .map(id -> dataStreamIndex.getEntry(id))
                .filter(Objects::nonNull);
        }
        
        // apply post filters
        if (filter.getResultTimes() != null)
        {
            resultStream = resultStream.filter(e -> {
                Range<Instant> resultTimeRange = obsStore.getDataStreamResultTimeRange(e.getKey());
                return resultTimeRange.isConnected(filter.getResultTimes());
            });
        }
        
        if (filter.getObservedProperties() != null)
            resultStream = resultStream.filter(e -> filter.testObservedProperty(e.getValue()));
        
        if (filter.getValuePredicate() != null)
            resultStream = resultStream.filter(e -> filter.testValuePredicate(e.getValue()));
        
        // apply limit
        if (filter.getLimit() < Long.MAX_VALUE)
            resultStream = resultStream.limit(filter.getLimit());
        
        return resultStream;
    }


    @Override
    public synchronized void clear()
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                obsStore.clear();
                dataStreamByProcIndex.clear();
                dataStreamIndex.clear();
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public boolean containsKey(Object key)
    {
        return dataStreamIndex.containsKey(key);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return dataStreamIndex.containsValue(val);
    }


    @Override
    public Set<Entry<Long, DataStreamInfo>> entrySet()
    {
        return dataStreamIndex.entrySet();
    }


    @Override
    public DataStreamInfo get(Object key)
    {
        return dataStreamIndex.get(key);
    }


    @Override
    public boolean isEmpty()
    {
        return dataStreamIndex.isEmpty();
    }


    @Override
    public Set<Long> keySet()
    {
        return dataStreamIndex.keySet();
    }


    @Override
    public synchronized DataStreamInfo put(Long key, DataStreamInfo value)
    {
        Asserts.checkNotNull(key, "key");
        Asserts.checkNotNull(value, DataStreamInfo.class);
        Asserts.checkNotNull(value.getProcedure(), "procedureID");
        Asserts.checkNotNull(value.getOutputName(), "outputName");
        
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                // if procedure UID was not provided, try to get it from linked procedure store
                String procUID = value.getProcedure().getUniqueID();
                if (procUID == null)
                {
                    procUID = obsStore.fetchFeatureUID(value.getProcedure().getInternalID(), obsStore.procedureStore);
                    Asserts.checkArgument(!Strings.isNullOrEmpty(procUID),
                        "Procedure UID must be known when inserting a new data stream");
                    value.getProcedure().setUniqueID(procUID);
                }
            
                // add to index
                DataStreamInfo oldValue = dataStreamIndex.put(key, value);
                
                // add to secondary index
                MVDataStreamProcKey procKey = new MVDataStreamProcKey(key,
                    value.getProcedure().getInternalID(),
                    value.getOutputName(),
                    value.getRecordVersion());
                dataStreamByProcIndex.put(procKey, Boolean.TRUE);
                
                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public synchronized DataStreamInfo remove(Object key)
    {
        Asserts.checkArgument(key instanceof Long);
        
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                // remove from main index                
                DataStreamInfo oldValue = dataStreamIndex.remove(key);
                if (oldValue == null)
                    return null;
                
                // remove entry in secondary index
                dataStreamByProcIndex.remove(new MVDataStreamProcKey(
                    oldValue.getProcedure().getInternalID(),
                    oldValue.getOutputName(),
                    oldValue.getRecordVersion()));
                
                // remove all obs
                removeAllObsAndSeries((Long)key);
                
                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }
    
    
    protected void removeAllObsAndSeries(Long dataStreamID)
    {
        // first remove all associated observations
        obsStore.removeEntries(new ObsFilter.Builder()
            .withDataStreams(dataStreamID)
            .build());
        
        // also remove series
        MVObsSeriesKey first = new MVObsSeriesKey(dataStreamID, 0, Instant.MIN);
        MVObsSeriesKey last = new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, Instant.MAX);
        if (first.dataStreamID == dataStreamID)
        {
            new RangeCursor<>(obsStore.obsSeriesMainIndex, first, last).keyStream().forEach(k -> {
                obsStore.obsSeriesMainIndex.remove(k);
                obsStore.obsSeriesByFoiIndex.remove(k);
            });
        }
    }


    @Override
    public int size()
    {
        return dataStreamIndex.size();
    }


    @Override
    public Collection<DataStreamInfo> values()
    {
        return dataStreamIndex.values();
    }


    @Override
    public void commit()
    {
        obsStore.commit();        
    }


    @Override
    public void backup(OutputStream is) throws IOException
    {
        throw new UnsupportedOperationException("Call backup on the parent observation store");
    }


    @Override
    public void restore(InputStream os) throws IOException
    {
        throw new UnsupportedOperationException("Call restore on the parent observation store");
    }


    @Override
    public boolean isReadSupported()
    {
        return true;
    }


    @Override
    public boolean isWriteSupported()
    {
        return true;
    }

}
