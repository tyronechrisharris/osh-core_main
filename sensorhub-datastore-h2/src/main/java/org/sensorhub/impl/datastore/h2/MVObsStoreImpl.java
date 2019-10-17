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
import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.IProcedureStore;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RangeCursor;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.FeatureFilter;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.sensorhub.api.datastore.IFeatureStore;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.api.datastore.ObsKey;
import org.sensorhub.api.datastore.ObsStats;
import org.sensorhub.api.datastore.ObsStatsQuery;
import org.sensorhub.api.datastore.ObsData;
import org.sensorhub.api.datastore.RangeFilter;
import org.sensorhub.impl.datastore.stream.MergeSortSpliterator;
import org.vast.util.Asserts;
import com.google.common.base.Strings;
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
    protected MVBTreeMap<MVObsKey, ObsData> obsRecordsIndex;
    protected MVBTreeMap<MVObsSeriesKey, MVObsSeriesInfo> obsSeriesMainIndex;
    protected MVBTreeMap<MVObsSeriesKey, Boolean> obsSeriesByFoiIndex;
    
    protected MVFoiStoreImpl foiStore;
    protected MVProcedureStoreImpl procedureStore;
    protected int maxSelectedSeriesOnJoin = 200;
    
    
    private MVObsStoreImpl()
    {
    }


    /**
     * Opens an existing obs store with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param dataStoreName name of data store to open
     * @param procedureStore associated procedure descriptions data store
     * @param foiStore associated FOIs data store
     * @return The existing datastore instance 
     */
    public static MVObsStoreImpl open(MVStore mvStore, String dataStoreName, MVProcedureStoreImpl procedureStore, MVFoiStoreImpl foiStore)
    {
        MVDataStoreInfo dataStoreInfo = (MVDataStoreInfo)H2Utils.loadDataStoreInfo(mvStore, dataStoreName);
        return new MVObsStoreImpl().init(mvStore, procedureStore, foiStore, dataStoreInfo);
    }
    
    
    /**
     * Create a new obs store with the provided info
     * @param mvStore MVStore instance where the data store maps will be created
     * @param procedureStore associated procedure descriptions data store
     * @param foiStore associated FOIs data store
     * @param dataStoreInfo new data store info
     * @return The new datastore instance 
     */
    public static MVObsStoreImpl create(MVStore mvStore, MVProcedureStoreImpl procedureStore, MVFoiStoreImpl foiStore, MVDataStoreInfo dataStoreInfo)
    {
        H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        return new MVObsStoreImpl().init(mvStore, procedureStore, foiStore, dataStoreInfo);
    }
    
    
    private MVObsStoreImpl init(MVStore mvStore, MVProcedureStoreImpl procedureStore, MVFoiStoreImpl foiStore, MVDataStoreInfo dataStoreInfo)
    {
        this.mvStore = Asserts.checkNotNull(mvStore, MVStore.class);
        this.dataStoreInfo = Asserts.checkNotNull(dataStoreInfo, MVDataStoreInfo.class);
        this.foiStore = Asserts.checkNotNull(foiStore, IFoiStore.class);
        this.procedureStore = Asserts.checkNotNull(procedureStore, IProcedureStore.class);
        this.dataStreamStore = new MVDataStreamStoreImpl(this, null); 
                        
        // open observation map
        String mapName = OBS_RECORDS_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsRecordsIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVObsKey, ObsData>()
                .keyType(new MVObsKeyDataType())
                .valueType(new ObsDataType()));
        
        // open observation series map
        mapName = OBS_SERIES_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsSeriesMainIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVObsSeriesKey, MVObsSeriesInfo>()
                .keyType(new MVObsSeriesKeyByDataStreamDataType())
                .valueType(new MVObsSeriesInfoDataType()));
        
        mapName = OBS_SERIES_FOI_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsSeriesByFoiIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVObsSeriesKey, Boolean>()
                .keyType(new MVObsSeriesKeyByFoiDataType())
                .valueType(new MVVoidDataType()));
        
        // link all 3 stores together to enable JOIN queries
        foiStore.linkTo(this);
        procedureStore.linkTo(this);
        
        return this;
    }


    @Override
    public String getDatastoreName()
    {
        return dataStoreInfo.getName();
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        return dataStoreInfo.getZoneOffset();
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
    
    
    public Stream<Long> selectDataStreamIDs(DataStreamFilter filter)
    {
        if (filter.getInternalIDs() != null &&
            filter.getObservedProperties() == null)
        {
            // if only internal IDs were specified, no need to search the feature store
            return filter.getInternalIDs().stream();
        }
        else
        {
            // otherwise select all datastream keys matching the filter
            return dataStreamStore.selectKeys(filter);
        }
    }
    
    
    public Stream<Long> selectFeatureIDs(IFeatureStore<FeatureKey,?> featureStore, FeatureFilter filter)
    {
        if (filter.getInternalIDs() != null &&
            filter.getInternalIDs().isSet() &&
            filter.getLocationFilter() == null)
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
            return featureStore.selectKeys(filter)
                .map(k -> k.getInternalID())
                .distinct();
        }
    }
    
    
    Stream<MVObsSeriesInfo> getAllObsSeries(RangeFilter<Instant> resultTimeRange)
    {
        MVObsSeriesKey first = new MVObsSeriesKey(0, 0, resultTimeRange.getMin());
        MVObsSeriesKey last = new MVObsSeriesKey(Long.MAX_VALUE, Long.MAX_VALUE, resultTimeRange.getMax());        
        RangeCursor<MVObsSeriesKey, MVObsSeriesInfo> cursor = new RangeCursor<>(obsSeriesMainIndex, first, last);
        
        return cursor.entryStream()
            .filter(e -> resultTimeRange.test(e.getKey().resultTime))
            .map(e -> {
                e.getValue().key = e.getKey();
                return e.getValue();
            });
    }
    
    
    Stream<MVObsSeriesInfo> getObsSeriesByDataStream(long dataStreamID, Range<Instant> resultTimeRange, boolean lastResultOnly)
    {
        // special case when last result is requested
        if (lastResultOnly)
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
    
    
    Stream<MVObsSeriesInfo> getObsSeriesByFoi(long foiID, Range<Instant> resultTimeRange, boolean lastResultOnly)
    {
        // special case when last result is requested
        if (lastResultOnly)
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
    
    
    RangeCursor<MVObsKey, ObsData> getObsCursor(long seriesID, Range<Instant> phenomenonTimeRange)
    {
        MVObsKey first = new MVObsKey(seriesID, phenomenonTimeRange.lowerEndpoint());
        MVObsKey last = new MVObsKey(seriesID, phenomenonTimeRange.upperEndpoint());        
        return new RangeCursor<>(obsRecordsIndex, first, last);
    }
    
    
    Stream<Entry<ObsKey, ObsData>> getObsStream(MVObsSeriesInfo series, Range<Instant> resultTimeRange, Range<Instant> phenomenonTimeRange, boolean lastResultOnly)
    {
        // if series is a special case where all obs have resultTime = phenomenonTime
        if (series.key.resultTime == Instant.MIN)
        {
            // if request is for last result only, get the obs with latest phenomenon time
            if (lastResultOnly)
            {
                MVObsKey maxKey = new MVObsKey(series.id, Instant.MAX);      
                Entry<MVObsKey, ObsData> e = obsRecordsIndex.floorEntry(maxKey);
                if (e.getKey().seriesID == series.id)
                    return Stream.of(mapToFullObsEntry(series, e));
                else
                    return Stream.empty();
            }
            
            // else further restrict the requested time range using result time filter
            phenomenonTimeRange = resultTimeRange.intersection(phenomenonTimeRange);
        }
        
        // scan using a cursor on main obs index
        // recreating full entries in the process
        RangeCursor<MVObsKey, ObsData> cursor = getObsCursor(series.id, phenomenonTimeRange);
        return cursor.entryStream()
            .map(e -> {
                return mapToFullObsEntry(series, e);
            });
    }
    
    
    Entry<ObsKey, ObsData> mapToFullObsEntry(MVObsSeriesInfo obsSeries, Entry<MVObsKey, ObsData> internalEntry)
    {
        ObsKey key = mapToFullObsKey(obsSeries, internalEntry.getKey());            
        return new DataUtils.MapEntry<>(key, internalEntry.getValue());
    }
    
    
    ObsKey mapToFullObsKey(MVObsSeriesInfo obsSeries, MVObsKey internalKey)
    {
        /*return ObsKey.builder()
            .withPhenomenonTime(internalKey.phenomenonTime)
            .withResultTime(obsSeries.key.resultTime)
            .withProcedureKey(FeatureKey.builder()
                .withInternalID(obsSeries.key.dataStreamID)
                .withUniqueID(obsSeries.procUID)
                .build())
            .withFoiKey(FeatureKey.builder()
                .withInternalID(obsSeries.key.foiID)
                .withUniqueID(obsSeries.foiUID)
                .build())
            .build();*/
        return internalKey.setSeriesInfo(obsSeries);
    }


    @Override
    public Stream<Entry<ObsKey, ObsData>> selectEntries(ObsFilter filter)
    {        
        // get phenomenon time filter
        final RangeFilter<Instant> phenomenonTimeFilter;
        if (filter.getPhenomenonTime() != null)
            phenomenonTimeFilter = filter.getPhenomenonTime();
        else
            phenomenonTimeFilter = H2Utils.ALL_TIMES_FILTER;
        
        // get result time filter
        final RangeFilter<Instant> resultTimeFilter;
        if (filter.getResultTime() != null)
            resultTimeFilter = filter.getResultTime();
        else
            resultTimeFilter = H2Utils.ALL_TIMES_FILTER;
        boolean lastResultOnly = resultTimeFilter.getMin() == Instant.MAX && resultTimeFilter.getMax() == Instant.MAX;
        
        // handle different cases of JOIN with datastreams and FOIs
        // prepare stream of matching obs series
        Stream<MVObsSeriesInfo> obsSeries = null;
        if (filter.getFoiFilter() == null) // no FOI filter set
        {
            if (filter.getDataStreamFilter() != null)
            {
                // stream directly from list of selected datastreams
                obsSeries = selectDataStreamIDs(filter.getDataStreamFilter())
                    .flatMap(id -> {
                        return getObsSeriesByDataStream(id, resultTimeFilter.getRange(), lastResultOnly);
                    });
            }
            else
            {
                // if no datastream or FOI selected, scan all series
                obsSeries = getAllObsSeries(resultTimeFilter);
            }
        }
        else if (filter.getDataStreamFilter() == null) // no datastream filter set
        {
            if (filter.getFoiFilter() != null)
            {
                // stream directly from list of selected fois
                obsSeries = selectFeatureIDs(foiStore, filter.getFoiFilter())
                    .flatMap(id -> {
                        return getObsSeriesByFoi(id, resultTimeFilter.getRange(), lastResultOnly);
                    });
            }
        }
        else // both datastream and FOI filters are set
        {
            // create set of selected datastreams
            AtomicInteger counter = new AtomicInteger();
            Set<Long> dataStreamIDs = selectDataStreamIDs(filter.getDataStreamFilter())
                .peek(s -> {
                    // make sure set size cannot go over a threshold
                    if (counter.incrementAndGet() >= 100*maxSelectedSeriesOnJoin)
                        throw new IllegalStateException("Too many datastreams selected. Please refine your filter");                    
                })
                .collect(Collectors.toSet());

            if (dataStreamIDs.isEmpty())
                return Stream.empty();
            
            // stream from fois and filter on datastream IDs
            obsSeries = selectFeatureIDs(foiStore, filter.getFoiFilter())
                .flatMap(id -> {
                    return getObsSeriesByFoi(id, resultTimeFilter.getRange(), lastResultOnly)
                        .filter(s -> dataStreamIDs.contains(s.key.dataStreamID));
                });
        }
        
        // create obs streams for each selected series
        // and keep all spliterators in array list
        final ArrayList<Spliterator<Entry<ObsKey, ObsData>>> obsIterators = new ArrayList<>(100);
        obsIterators.add(obsSeries
            .peek(s -> {
                // make sure list size cannot go over a threshold
                if (obsIterators.size() >= maxSelectedSeriesOnJoin)
                    throw new IllegalStateException("Too many datastreams or features of interest selected. Please refine your filter");
            })
            .flatMap(series -> {
                Stream<Entry<ObsKey, ObsData>> obsStream = getObsStream(series, 
                    resultTimeFilter.getRange(),
                    phenomenonTimeFilter.getRange(),
                    lastResultOnly);
                return getPostFilteredResultStream(obsStream, filter);
            })
            .spliterator());        
        
        // TODO group by result time when series with different result times are selected
        
        // stream and merge obs from all selected datastreams and time periods
        MergeSortSpliterator<Entry<ObsKey, ObsData>> mergeSortIt = new MergeSortSpliterator<>(obsIterators,
                (e1, e2) -> e1.getKey().getPhenomenonTime().compareTo(e2.getKey().getPhenomenonTime()));         
               
        // stream output of merge sort iterator + apply limit        
        return StreamSupport.stream(mergeSortIt, false)
            .limit(filter.getLimit());
    }
    
    
    Stream<Entry<ObsKey, ObsData>> getPostFilteredResultStream(Stream<Entry<ObsKey, ObsData>> resultStream, ObsFilter filter)
    {
        if (filter.getKeyPredicate() != null)
            resultStream = resultStream.filter(e -> filter.testKeyPredicate(e.getKey()));
        
        if (filter.getValuePredicate() != null)
            resultStream = resultStream.filter(e -> filter.testValuePredicate(e.getValue()));
        
        return resultStream;
    }
    

    @Override
    public Stream<DataBlock> selectResults(ObsFilter filter)
    {
        return select(filter).map(obs -> obs.getResult());
    }
        
    
    Range<Instant> getDataStreamResultTimeRange(long dataStreamID)
    {
        MVObsSeriesKey firstKey = obsSeriesMainIndex.ceilingKey(new MVObsSeriesKey(dataStreamID, 0, Instant.MIN));
        MVObsSeriesKey lastKey = obsSeriesMainIndex.floorKey(new MVObsSeriesKey(dataStreamID, Long.MAX_VALUE, Instant.MAX));
        return Range.closed(firstKey.resultTime, lastKey.resultTime);
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
    
    
    MVObsSeriesKey getObsSeriesKey(ObsKey key)
    {
        return new MVObsSeriesKey(
            key.getDataStreamID(),
            key.getFoiID() == null ? 0 : key.getFoiID().getInternalID(),
            key.getResultTime().equals(key.getPhenomenonTime()) ? Instant.MIN : key.getResultTime());
    }
    
    
    MVObsKey getMVObsKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof ObsKey, "key must be a ObsKey");
        ObsKey key = (ObsKey)keyObj;
        
        MVObsSeriesKey seriesKey = getObsSeriesKey(key);
        MVObsSeriesInfo series = obsSeriesMainIndex.get(seriesKey);
        if (series == null)
            return null;
        
        return new MVObsKey(series.id, key.getPhenomenonTime());
    }


    @Override
    public boolean containsKey(Object key)
    {
        MVObsKey obsKey = getMVObsKey(key);
        return obsKey == null ? false : obsRecordsIndex.containsKey(obsKey);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return obsRecordsIndex.containsValue(value);
    }


    @Override
    public ObsData get(Object key)
    {
        MVObsKey obsKey = getMVObsKey(key);
        return obsKey == null ? null : obsRecordsIndex.get(obsKey);
    }


    @Override
    public boolean isEmpty()
    {
        return obsRecordsIndex.isEmpty();
    }


    @Override
    public Set<Entry<ObsKey, ObsData>> entrySet()
    {
        return new AbstractSet<>() {        
            @Override
            public Iterator<Entry<ObsKey, ObsData>> iterator() {
                return getAllObsSeries(H2Utils.ALL_TIMES_FILTER)
                    .flatMap(series -> {
                        RangeCursor<MVObsKey, ObsData> cursor = getObsCursor(series.id, H2Utils.ALL_TIMES_RANGE);
                        return cursor.entryStream().map(e -> {
                            return mapToFullObsEntry(series, e);
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
    public Set<ObsKey> keySet()
    {
        return new AbstractSet<>() {        
            @Override
            public Iterator<ObsKey> iterator() {
                return getAllObsSeries(H2Utils.ALL_TIMES_FILTER)
                    .flatMap(series -> {
                        RangeCursor<MVObsKey, ObsData> cursor = getObsCursor(series.id, H2Utils.ALL_TIMES_RANGE);
                        return cursor.keyStream().map(e -> {
                            return mapToFullObsKey(series, e);
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
    public ObsData put(ObsKey key, ObsData value)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVObsSeriesKey seriesKey = new MVObsSeriesKey(
                    key.getDataStreamID(),
                    key.getFoiID() == null ? 0 : key.getFoiID().getInternalID(),
                    key.getResultTime().equals(key.getPhenomenonTime()) ? Instant.MIN : key.getResultTime());
                
                MVObsSeriesInfo series = obsSeriesMainIndex.computeIfAbsent(seriesKey, k -> {
                    
                    // if foi UID was not provided, try to get it from linked datastore
                    String foiUID = null;
                    if (key.getFoiID() != null && key.getFoiID().getInternalID() > 0)
                    {
                        foiUID = key.getFoiID().getUniqueID();
                        if (foiUID == null)
                            foiUID = fetchFeatureUID(key.getFoiID().getInternalID(), foiStore);
                        
                        Asserts.checkArgument(!Strings.isNullOrEmpty(foiUID),
                            "Foi UID must be known when inserting a new observation");
                    }
                    
                    // also update the FOI to procedure mapping if needed
                    if (key.getFoiID() != null)
                        obsSeriesByFoiIndex.putIfAbsent(seriesKey, Boolean.TRUE);
                    
                    return new MVObsSeriesInfo(
                        obsRecordsIndex.isEmpty() ? 1 : obsRecordsIndex.lastKey().seriesID + 1, foiUID);
                });        
                
                // add to main obs index
                MVObsKey obsKey = new MVObsKey(series.id, key.getPhenomenonTime());
                return obsRecordsIndex.put(obsKey, value);
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected String fetchFeatureUID(long internalID, IFeatureStore dataStore)
    {
        if (dataStore == null || internalID <= 0)
            return null;
        
        var key = new FeatureKey(internalID);
        FeatureId id = dataStore.getFeatureID(key);
        return id != null ? id.getUniqueID() : null;
    }


    @Override
    public ObsData remove(Object keyObj)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVObsKey key = getMVObsKey(keyObj);
                ObsData oldObs = obsRecordsIndex.remove(key);
                
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
    public Collection<ObsData> values()
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
}
