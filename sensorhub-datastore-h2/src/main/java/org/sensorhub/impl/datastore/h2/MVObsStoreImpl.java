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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RangeCursor;
import org.h2.mvstore.WriteBuffer;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsStats;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.stream.MergeSortSpliterator;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.collect.Range;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Implementation of obs store based on H2 MVStore, capable of handling a
 * single result type.
 * </p><p>
 * Note that the store can contain data for several data streams as long as
 * they share the same result types. Thus no separate metadata is kept for 
 * individual data streams.
 * </p><p>
 * Several instances of this store can be contained in the same MVStore
 * as long as they have different names.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
public class MVObsStoreImpl implements IObsStore
{
    private static final String OBS_RECORDS_MAP_NAME = "@obs_records";
    private static final String OBS_SERIES_MAP_NAME = "@obs_series";
    private static final String OBS_SERIES_FOI_MAP_NAME = "@obs_series_foi";
    //static final Instant LOWEST_TIME_KEY = Instant.MIN.plusSeconds(1);
    //static final Instant HIGHEST_TIME_KEY = Instant.MAX;
    
    protected MVStore mvStore;
    protected MVDataStoreInfo dataStoreInfo;
    protected MVDataStreamStoreImpl dataStreamStore;
    protected MVBTreeMap<MVObsKey, IObsData> obsRecordsIndex;
    protected MVBTreeMap<MVObsSeriesKey, MVObsSeriesInfo> obsSeriesMainIndex;
    protected MVBTreeMap<MVObsSeriesKey, Boolean> obsSeriesByFoiIndex;
    
    protected IFoiStore foiStore;
    protected int maxSelectedSeriesOnJoin = 200;
    
    
    private MVObsStoreImpl()
    {
    }


    /**
     * Opens an existing obs store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param procedureStore associated procedure descriptions data store
     * @param foiStore associated FOIs data store
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVObsStoreImpl open(MVStore mvStore, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        return new MVObsStoreImpl().init(mvStore, dataStoreInfo);
    }
    
    
    private MVObsStoreImpl init(MVStore mvStore, MVDataStoreInfo dataStoreInfo)
    {
        this.mvStore = Asserts.checkNotNull(mvStore, MVStore.class);
        this.dataStoreInfo = Asserts.checkNotNull(dataStoreInfo, MVDataStoreInfo.class);
        this.dataStreamStore = new MVDataStreamStoreImpl(this, null); 
                        
        // open observation map
        String mapName = OBS_RECORDS_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsRecordsIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVObsKey, IObsData>()
                .keyType(new MVObsKeyDataType())
                .valueType(new MVObsDataType()));
        
        // open observation series map
        mapName = OBS_SERIES_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsSeriesMainIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVObsSeriesKey, MVObsSeriesInfo>()
                .keyType(new MVObsSeriesKeyByDataStreamDataType())
                .valueType(new MVObsSeriesInfoDataType()));
        
        mapName = OBS_SERIES_FOI_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsSeriesByFoiIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVObsSeriesKey, Boolean>()
                .keyType(new MVObsSeriesKeyByFoiDataType())
                .valueType(new MVVoidDataType()));
        
        return this;
    }


    @Override
    public String getDatastoreName()
    {
        return dataStoreInfo.getName();
    }
    

    @Override
    public IDataStreamStore getDataStreams()
    {
        return dataStreamStore;
    }


    @Override
    public long getNumRecords()
    {
        return obsRecordsIndex.sizeAsLong();
    }
    
    
    Stream<MVObsSeriesInfo> getAllObsSeries(Range<Instant> resultTimeRange)
    {
        MVObsSeriesKey first = new MVObsSeriesKey(0, 0, resultTimeRange.lowerEndpoint());
        MVObsSeriesKey last = new MVObsSeriesKey(Long.MAX_VALUE, Long.MAX_VALUE, resultTimeRange.upperEndpoint());        
        RangeCursor<MVObsSeriesKey, MVObsSeriesInfo> cursor = new RangeCursor<>(obsSeriesMainIndex, first, last);
        
        return cursor.entryStream()
            .filter(e -> resultTimeRange.contains(e.getKey().resultTime))
            .map(e -> {
                e.getValue().key = e.getKey();
                return e.getValue();
            });
    }
    
    
    Stream<MVObsSeriesInfo> getObsSeriesByDataStream(long dataStreamID, Range<Instant> resultTimeRange, boolean latestResultOnly)
    {
        // special case when latest result is requested
        if (latestResultOnly)
        {
            MVObsSeriesKey key = new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, Instant.MAX);
            MVObsSeriesKey lastKey = obsSeriesMainIndex.floorKey(key);
            if (lastKey.dataStreamID != dataStreamID)
                return null;
            resultTimeRange = Range.singleton(lastKey.resultTime);
        }
       
        // scan series for all FOIs of the selected procedure and result times
        MVObsSeriesKey first = new MVObsSeriesKey(dataStreamID, 0, resultTimeRange.lowerEndpoint());
        MVObsSeriesKey last = new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, resultTimeRange.upperEndpoint());
        RangeCursor<MVObsSeriesKey, MVObsSeriesInfo> cursor = new RangeCursor<>(obsSeriesMainIndex, first, last);
        
        return cursor.entryStream()
            .map(e -> {
                MVObsSeriesInfo series = e.getValue();
                series.key = e.getKey();
                return series;
            });
    }
    
    
    Stream<MVObsSeriesInfo> getObsSeriesByFoi(long foiID, Range<Instant> resultTimeRange, boolean latestResultOnly)
    {
        // special case when latest result is requested
        if (latestResultOnly)
        {
            MVObsSeriesKey key = new MVObsSeriesKey(Long.MAX_VALUE, foiID, Instant.MAX);
            MVObsSeriesKey lastKey = obsSeriesByFoiIndex.floorKey(key);
            if (lastKey.foiID != foiID)
                return null;
            resultTimeRange = Range.singleton(lastKey.resultTime);
        }
       
        // scan series for all procedures that produced observations of the selected FOI
        final Range<Instant> finalResultTimeRange = resultTimeRange;
        MVObsSeriesKey first = new MVObsSeriesKey(0, foiID, resultTimeRange.lowerEndpoint());
        MVObsSeriesKey last = new MVObsSeriesKey(Long.MAX_VALUE, foiID, resultTimeRange.upperEndpoint());
        RangeCursor<MVObsSeriesKey, Boolean> cursor = new RangeCursor<>(obsSeriesByFoiIndex, first, last);
        
        return cursor.keyStream()
            .filter(k -> finalResultTimeRange.test(k.resultTime))
            .map(k -> {
                MVObsSeriesInfo series = obsSeriesMainIndex.get(k);
                series.key = k;
                return series;
            });
    }
    
    
    Stream<MVObsSeriesInfo> getObsSeriesByDataStreamAndFoi(long dataStreamID, long foiID, Range<Instant> resultTimeRange, boolean latestResultOnly)
    {
        // special case when latest result is requested
        if (latestResultOnly)
        {
            MVObsSeriesKey key = new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, Instant.MAX);
            MVObsSeriesKey lastKey = obsSeriesMainIndex.floorKey(key);
            if (lastKey.dataStreamID != dataStreamID)
                return null;
            resultTimeRange = Range.singleton(lastKey.resultTime);
        }
       
        // scan series for all FOIs of the selected procedure and result times
        MVObsSeriesKey first = new MVObsSeriesKey(dataStreamID, 0, resultTimeRange.lowerEndpoint());
        MVObsSeriesKey last = new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, resultTimeRange.upperEndpoint());
        RangeCursor<MVObsSeriesKey, MVObsSeriesInfo> cursor = new RangeCursor<>(obsSeriesMainIndex, first, last);
        
        return cursor.entryStream()
            .map(e -> {
                MVObsSeriesInfo series = e.getValue();
                series.key = e.getKey();
                return series;
            });
    }
    
    
    RangeCursor<MVObsKey, IObsData> getObsCursor(long seriesID, Range<Instant> phenomenonTimeRange)
    {
        MVObsKey first = new MVObsKey(seriesID, phenomenonTimeRange.lowerEndpoint());
        MVObsKey last = new MVObsKey(seriesID, phenomenonTimeRange.upperEndpoint());        
        return new RangeCursor<>(obsRecordsIndex, first, last);
    }
    
    
    Stream<Entry<BigInteger, IObsData>> getObsStream(MVObsSeriesInfo series, Range<Instant> resultTimeRange, Range<Instant> phenomenonTimeRange, boolean latestResultOnly)
    {
        // if series is a special case where all obs have resultTime = phenomenonTime
        if (series.key.resultTime == Instant.MIN)
        {
            // if request is for latest result only, get the obs with latest phenomenon time
            if (latestResultOnly)
            {
                MVObsKey maxKey = new MVObsKey(series.id, Instant.MAX);      
                Entry<MVObsKey, IObsData> e = obsRecordsIndex.floorEntry(maxKey);
                if (e.getKey().seriesID == series.id)
                    return Stream.of(mapToPublicEntry(e));
                else
                    return Stream.empty();
            }
            
            // else further restrict the requested time range using result time filter
            phenomenonTimeRange = resultTimeRange.intersection(phenomenonTimeRange);
        }
        
        // scan using a cursor on main obs index
        // recreating full entries in the process
        RangeCursor<MVObsKey, IObsData> cursor = getObsCursor(series.id, phenomenonTimeRange);
        return cursor.entryStream()
            .map(e -> {
                return mapToPublicEntry(e);
            });
    }
    
    
    BigInteger mapToPublicKey(MVObsKey internalKey)
    {
        // compute internal ID
        WriteBuffer buf = new WriteBuffer(24); // seriesID + timestamp seconds + nanos
        DataUtils.writeVarLong(buf.getBuffer(), internalKey.getSeriesID());
        H2Utils.writeInstant(buf, internalKey.getPhenomenonTime());
        return new BigInteger(buf.getBuffer().array(), 0, buf.position());
    }
    
    
    MVObsKey mapToInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger, "key must be a BigInteger");
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            ByteBuffer buf = ByteBuffer.wrap(key.toByteArray());
            long seriesID = DataUtils.readVarLong(buf);
            Instant phenomenonTime = H2Utils.readInstant(buf);
            
            return new MVObsKey(seriesID, phenomenonTime);
        }
        catch (Exception e)
        {
            // invalid bigint key
            return null;
        }
    }
    
    
    Entry<BigInteger, IObsData> mapToPublicEntry(Entry<MVObsKey, IObsData> internalEntry)
    {
        BigInteger obsID = mapToPublicKey(internalEntry.getKey());
        return new DataUtils.MapEntry<>(obsID, internalEntry.getValue());
    }


    @Override
    public Stream<Entry<BigInteger, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields)
    {        
        // get phenomenon time filter
        var phenomenonTimeFilter = filter.getPhenomenonTime() != null ?
            filter.getPhenomenonTime().getRange() : H2Utils.ALL_TIMES_RANGE;
        
        // get result time filter
        var resultTimeFilter = filter.getResultTime() != null ?
            filter.getResultTime().getRange() : H2Utils.ALL_TIMES_RANGE;
        boolean latestResultOnly = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        
        // stream obs directly in case of filtering by internal IDs
        if (filter.getInternalIDs() != null)
        {
            var obsStream = filter.getInternalIDs().stream()
                .map(k -> mapToInternalKey(k))
                .map(k -> obsRecordsIndex.getEntry(k))
                .map(e -> mapToPublicEntry(e));
            
            return getPostFilteredResultStream(obsStream, filter);
        }
        
        // otherwise prepare stream of matching obs series
        Stream<MVObsSeriesInfo> obsSeries = null;
        
        // if no datastream nor FOI filter used, scan all obs
        if (filter.getDataStreamFilter() == null && filter.getFoiFilter() == null)
        {
            obsSeries = getAllObsSeries(resultTimeFilter);
        }
        
        // only datastream filter used
        else if (filter.getDataStreamFilter() != null && filter.getFoiFilter() == null)
        {
            // stream directly from list of selected datastreams
            obsSeries = DataStoreUtils.selectDataStreamIDs(dataStreamStore, filter.getDataStreamFilter())
                .flatMap(id -> getObsSeriesByDataStream(id, resultTimeFilter, latestResultOnly));
        }
        
        // only FOI filter used
        else if (filter.getFoiFilter() != null && filter.getDataStreamFilter() == null)
        {
            // stream directly from list of selected fois
            obsSeries = DataStoreUtils.selectFeatureIDs(foiStore, filter.getFoiFilter())
                .flatMap(id -> getObsSeriesByFoi(id, resultTimeFilter, latestResultOnly));
        }
        
        // both datastream and FOI filters used
        else
        {
            // create set of selected datastreams
            AtomicInteger counter = new AtomicInteger();
            Set<Long> dataStreamIDs = DataStoreUtils.selectDataStreamIDs(dataStreamStore, filter.getDataStreamFilter())
                .peek(s -> {
                    // make sure set size cannot go over a threshold
                    if (counter.incrementAndGet() >= 100*maxSelectedSeriesOnJoin)
                        throw new IllegalStateException("Too many datastreams selected. Please refine your filter");                    
                })
                .collect(Collectors.toSet());

            if (dataStreamIDs.isEmpty())
                return Stream.empty();
            
            // stream from fois and filter on datastream IDs
            obsSeries = DataStoreUtils.selectFeatureIDs(foiStore, filter.getFoiFilter())
                .flatMap(id -> {
                    return getObsSeriesByFoi(id, resultTimeFilter, latestResultOnly)
                        .filter(s -> dataStreamIDs.contains(s.key.dataStreamID));
                });
        }
        
        // create obs streams for each selected series
        // and keep all spliterators in array list
        final ArrayList<Spliterator<Entry<BigInteger, IObsData>>> obsIterators = new ArrayList<>(100);
        obsIterators.add(obsSeries
            .peek(s -> {
                // make sure list size cannot go over a threshold
                if (obsIterators.size() >= maxSelectedSeriesOnJoin)
                    throw new IllegalStateException("Too many datastreams or features of interest selected. Please refine your filter");
            })
            .flatMap(series -> {
                Stream<Entry<BigInteger, IObsData>> obsStream = getObsStream(series, 
                    resultTimeFilter,
                    phenomenonTimeFilter,
                    latestResultOnly);
                return getPostFilteredResultStream(obsStream, filter);
            })
            .spliterator());        
        
        // TODO group by result time when series with different result times are selected
        
        // stream and merge obs from all selected datastreams and time periods
        MergeSortSpliterator<Entry<BigInteger, IObsData>> mergeSortIt = new MergeSortSpliterator<>(obsIterators,
                (e1, e2) -> e1.getValue().getPhenomenonTime().compareTo(e2.getValue().getPhenomenonTime()));         
               
        // stream output of merge sort iterator + apply limit        
        return StreamSupport.stream(mergeSortIt, false)
            .limit(filter.getLimit());
    }
    
    
    Stream<Entry<BigInteger, IObsData>> getPostFilteredResultStream(Stream<Entry<BigInteger, IObsData>> resultStream, ObsFilter filter)
    {
        if (filter.getValuePredicate() != null)
            resultStream = resultStream.filter(e -> filter.testValuePredicate(e.getValue()));
        
        return resultStream;
    }
    
    
    @Override
    public Stream<Long> selectObservedFois(ObsFilter filter)
    {
        // get phenomenon time filter
        var phenomenonTimeFilter = filter.getPhenomenonTime() != null ?
            filter.getPhenomenonTime().getRange() : H2Utils.ALL_TIMES_RANGE;
            
        // get result time filter
        var resultTimeRange = filter.getResultTime() != null ?
            filter.getResultTime().getRange() : H2Utils.ALL_TIMES_RANGE;
        boolean latestResultOnly = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        
        if (filter.getDataStreamFilter() != null)
        {
            return DataStoreUtils.selectDataStreamIDs(dataStreamStore, filter.getDataStreamFilter())
                .flatMap(dsID -> {
                    return getObsSeriesByDataStream(dsID, resultTimeRange, latestResultOnly)
                        .filter(s -> {
                            var timeRange = getObsSeriesPhenomenonTimeRange(s.id);
                            return timeRange.isConnected(phenomenonTimeFilter);
                        })
                        .map(s -> s.key.foiID)
                        .distinct();
                });
        }
        
        return IObsStore.super.selectObservedFois(filter);
    }
    

    @Override
    public Stream<DataBlock> selectResults(ObsFilter filter)
    {
        return select(filter).map(obs -> obs.getResult());
    }
        
    
    TimeExtent getDataStreamResultTimeRange(long dataStreamID)
    {
        MVObsSeriesKey firstKey = obsSeriesMainIndex.ceilingKey(new MVObsSeriesKey(dataStreamID, 0, Instant.MIN));
        MVObsSeriesKey lastKey = obsSeriesMainIndex.floorKey(new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, Instant.MAX));
        
        if (firstKey == null || lastKey == null)
            return null;
        else if (firstKey.resultTime == Instant.MIN)
            return getDataStreamPhenomenonTimeRange(dataStreamID);
        else            
            return TimeExtent.period(firstKey.resultTime, lastKey.resultTime);
    }
    
    
    TimeExtent getDataStreamPhenomenonTimeRange(long dataStreamID)
    {
        Instant[] timeRange = new Instant[] {Instant.MAX, Instant.MIN};
        getObsSeriesByDataStream(dataStreamID, H2Utils.ALL_TIMES_RANGE, false)
            .forEach(s -> {
                MVObsKey firstKey = obsRecordsIndex.ceilingKey(new MVObsKey(s.id, Instant.MIN));
                MVObsKey lastKey = obsRecordsIndex.floorKey(new MVObsKey(s.id, Instant.MAX));
                
                if (firstKey != null && timeRange[0].isAfter(firstKey.phenomenonTime))
                    timeRange[0] = firstKey.phenomenonTime;
                if (lastKey != null && timeRange[1].isBefore(lastKey.phenomenonTime))
                    timeRange[1] = lastKey.phenomenonTime;
            });
        
        if (timeRange[0] == Instant.MAX || timeRange[1] == Instant.MIN)
            return null;
        else
            return TimeExtent.period(timeRange[0], timeRange[1]);
    }
    
    
    Range<Instant> getObsSeriesPhenomenonTimeRange(long seriesID)
    {
        MVObsKey firstKey = obsRecordsIndex.ceilingKey(new MVObsKey(seriesID, Instant.MIN));
        MVObsKey lastKey = obsRecordsIndex.floorKey(new MVObsKey(seriesID, Instant.MAX));
        return Range.closed(firstKey.phenomenonTime, lastKey.phenomenonTime);
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long countMatchingEntries(ObsFilter filter)
    {
        // TODO implement faster method for some special cases
        // i.e. when no predicates are used
        // can make use of H2 index counting feature
        
        return selectEntries(filter).limit(filter.getLimit()).count();
    }


    @Override
    public void clear()
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                obsRecordsIndex.clear();
                obsSeriesByFoiIndex.clear();
                obsSeriesMainIndex.clear();
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
        MVObsKey obsKey = mapToInternalKey(key);
        return obsKey == null ? false : obsRecordsIndex.containsKey(obsKey);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return obsRecordsIndex.containsValue(value);
    }


    @Override
    public IObsData get(Object key)
    {
        MVObsKey obsKey = mapToInternalKey(key);
        return obsKey == null ? null : obsRecordsIndex.get(obsKey);
    }


    @Override
    public boolean isEmpty()
    {
        return obsRecordsIndex.isEmpty();
    }


    @Override
    public Set<Entry<BigInteger, IObsData>> entrySet()
    {
        return new AbstractSet<>() {        
            @Override
            public Iterator<Entry<BigInteger, IObsData>> iterator() {
                return getAllObsSeries(H2Utils.ALL_TIMES_RANGE)
                    .flatMap(series -> {
                        RangeCursor<MVObsKey, IObsData> cursor = getObsCursor(series.id, H2Utils.ALL_TIMES_RANGE);
                        return cursor.entryStream().map(e -> {
                            return mapToPublicEntry(e);
                        });
                    }).iterator();
            }

            @Override
            public int size() {
                return obsRecordsIndex.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVObsStoreImpl.this.containsKey(o);
            }
        };
    }


    @Override
    public Set<BigInteger> keySet()
    {
        return new AbstractSet<>() {        
            @Override
            public Iterator<BigInteger> iterator() {
                return getAllObsSeries(H2Utils.ALL_TIMES_RANGE)
                    .flatMap(series -> {
                        RangeCursor<MVObsKey, IObsData> cursor = getObsCursor(series.id, H2Utils.ALL_TIMES_RANGE);
                        return cursor.keyStream().map(e -> {
                            return mapToPublicKey(e);
                        });
                    }).iterator();
            }

            @Override
            public int size() {
                return obsRecordsIndex.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVObsStoreImpl.this.containsKey(o);
            }
        };
    }
    
    
    @Override
    public BigInteger add(IObsData obs)
    {
        // check that datastream exists
        if (!dataStreamStore.containsKey(new DataStreamKey(obs.getDataStreamID())))
            throw new IllegalStateException("Unknown datastream: " + obs.getDataStreamID()); 
            
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVObsSeriesKey seriesKey = new MVObsSeriesKey(
                    obs.getDataStreamID(),
                    obs.getFoiID() == null ? 0 : obs.getFoiID().getInternalID(),
                    obs.getResultTime().equals(obs.getPhenomenonTime()) ? Instant.MIN : obs.getResultTime());
                
                MVObsSeriesInfo series = obsSeriesMainIndex.computeIfAbsent(seriesKey, k -> {                    
                    // also update the FOI to procedure mapping if needed
                    if (obs.getFoiID() != null)
                        obsSeriesByFoiIndex.putIfAbsent(seriesKey, Boolean.TRUE);
                    
                    return new MVObsSeriesInfo(
                        obsRecordsIndex.isEmpty() ? 1 : obsRecordsIndex.lastKey().seriesID + 1);
                });
                
                // add to main obs index
                MVObsKey obsKey = new MVObsKey(series.id, obs.getPhenomenonTime());
                obsRecordsIndex.put(obsKey, obs);
                
                return mapToPublicKey(obsKey);
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public IObsData put(BigInteger key, IObsData obs)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVObsKey obsKey = mapToInternalKey(key);
                IObsData oldObs = obsRecordsIndex.replace(obsKey, obs);
                if (oldObs == null)
                    throw new UnsupportedOperationException("put can only be used to update existing keys");
                return oldObs;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public IObsData remove(Object keyObj)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVObsKey key = mapToInternalKey(keyObj);
                IObsData oldObs = obsRecordsIndex.remove(key);
                
                // don't check and remove empty obs series here since in many cases they will be reused.
                // it can be done automatically during cleanup/compaction phase or with specific method.
                
                return oldObs;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }        
    }


    @Override
    public int size()
    {
        return obsRecordsIndex.size();
    }


    @Override
    public Collection<IObsData> values()
    {
        return obsRecordsIndex.values();
    }


    @Override
    public void commit()
    {
        obsRecordsIndex.getStore().commit();
        obsRecordsIndex.getStore().sync();
    }


    @Override
    public void backup(OutputStream output) throws IOException
    {
        // TODO Auto-generated method stub        
    }


    @Override
    public void restore(InputStream input) throws IOException
    {
        // TODO Auto-generated method stub        
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
    
    
    @Override
    public void linkTo(IFoiStore foiStore)
    {
        this.foiStore = Asserts.checkNotNull(foiStore, IFoiStore.class);
    }
}
