/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IObsStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RangeCursor;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IProcedureStore;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.api.datastore.ObsKey;
import org.sensorhub.api.datastore.ObsCluster;
import org.sensorhub.api.datastore.ObsClusterFilter;
import org.sensorhub.api.datastore.ObsData;
import org.sensorhub.api.datastore.RangeFilter;
import org.vast.util.Asserts;
import com.google.common.collect.Range;
import com.sensia.cloud.datastore.impl.stream.MergeSortSpliterator;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Obs Store implementation based on H2 MVStore.<br/>
 * Several obs stores can be contained in the same MVStore instance as long
 * as they have different names.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
public class MVObsStoreImpl implements IObsStore
{
    private static final String OBS_RECORDS_MAP_NAME = "@obs_records";
    private static final String OBS_CLUSTERS_MAP_NAME = "@obs_clusters";
    private static final String FOI_TIMES_MAP_NAME = "@obs_foi_times";
    static final Instant ALL_TIMES_KEY = Instant.MIN;
    static final Instant LOWEST_TIME_KEY = Instant.MIN.plusSeconds(1);
    static final Instant HIGHEST_TIME_KEY = Instant.MAX;
    
    MVObsStoreInfo dataStoreInfo;
    MVMap<ObsKey, ObsData> obsRecordsIndex;
    MVMap<ObsKey, ObsCluster> obsClustersIndex;
    MVMap<FeatureKey, FoiPeriod> foiTimesIndex;
    MVFoiStoreImpl foiStore;
    MVProcedureStoreImpl procedureStore;
    
    
    private MVObsStoreImpl()
    {        
    }


    /**
     * Opens an existing obs store with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param dataStoreName name of data store to open
     * @param foiStore associated FOIs data store
     * @param procedureStore associated procedure descriptions data store
     * @return The existing datastore instance 
     */
    public static MVObsStoreImpl open(MVStore mvStore, String dataStoreName, MVFoiStoreImpl foiStore, MVProcedureStoreImpl procedureStore)
    {
        MVObsStoreInfo dataStoreInfo = (MVObsStoreInfo)H2Utils.loadDataStoreInfo(mvStore, dataStoreName);
        return new MVObsStoreImpl().init(mvStore, dataStoreInfo, foiStore, procedureStore);
    }
    
    
    /**
     * Create a new obs store with the provided info
     * @param mvStore MVStore instance where the data store maps will be created
     * @param dataStoreInfo new data store info
     * @param foiStore associated FOIs data store
     * @param procedureStore associated procedure descriptions data store
     * @return The new datastore instance 
     */
    public static MVObsStoreImpl create(MVStore mvStore, MVObsStoreInfo dataStoreInfo, MVFoiStoreImpl foiStore, MVProcedureStoreImpl procedureStore)
    {
        H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        return new MVObsStoreImpl().init(mvStore, dataStoreInfo, foiStore, procedureStore);
    }
    
    
    private MVObsStoreImpl init(MVStore mvStore, MVObsStoreInfo dataStoreInfo, MVFoiStoreImpl foiStore, MVProcedureStoreImpl procedureStore)
    {
        this.dataStoreInfo = Asserts.checkNotNull(dataStoreInfo, MVObsStoreInfo.class);
        this.foiStore = Asserts.checkNotNull(foiStore, IFoiStore.class);
        this.procedureStore = Asserts.checkNotNull(procedureStore, IProcedureStore.class);
        
        // open observation map
        String mapName = OBS_RECORDS_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsRecordsIndex = mvStore.openMap(mapName, new MVMap.Builder<ObsKey, ObsData>()
                .keyType(new ObsKeyDataType())
                .valueType(new ObsDataType()));
        
        // open observation cluster map
        mapName = OBS_CLUSTERS_MAP_NAME + ":" + dataStoreInfo.name;
        this.obsClustersIndex = mvStore.openMap(mapName, new MVMap.Builder<ObsKey, ObsCluster>()
                .keyType(new ObsKeyDataType())
                .valueType(new ObsClusterDataType()));
        
        // open FOI times map
        mapName = FOI_TIMES_MAP_NAME + ":" + dataStoreInfo.name;
        this.foiTimesIndex = mvStore.openMap(mapName, new MVMap.Builder<FeatureKey, FoiPeriod>()
                .keyType(new MVFeatureKeyDataType())
                .valueType(new FoiPeriodDataType()));
        
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
    public long getNumRecords()
    {
        return obsRecordsIndex.sizeAsLong();
    }


    @Override
    public DataComponent getRecordDescription()
    {
        return dataStoreInfo.recordStruct;
    }


    @Override
    public DataEncoding getRecommendedEncoding()
    {
        return dataStoreInfo.recordEncoding;
    }
    
    
    /*
     * Retrieves obs periods (either phenomenon or result time periods),
     * filtered by procedures and FOIs and grouped by procedure
     */
    Stream<ObsCluster> getObsPeriodsGroupedByProcedure(ObsClusterFilter filter, Function<String, Stream<ObsCluster>> clusterStreamSupplier)
    {
        // if foi JOIN, prefetch selected foi IDs if needed
        Set<String> foiIDs;
        if (filter.getFeaturesOfInterest() != null && foiStore != null)
        {
            foiIDs = foiStore.selectKeys(filter.getFeaturesOfInterest())
                             .map(k -> k.getUniqueID())
                             .collect(Collectors.toSet());
            
            if (foiIDs.isEmpty())
                return Stream.empty();
            
            // TODO use FOI->proc index to restrict procedures?
            
        }
        else
            foiIDs = null;
        
        // if procedure JOIN, create stream of selected procedure IDs
        Stream<String> procIds;
        if (filter.getProcedures() != null && procedureStore != null)
            procIds = procedureStore.selectKeys(filter.getProcedures())
                                      .map(k -> k.getUniqueID());
        else
            procIds = Stream.of(ObsCluster.ALL_PROCEDURES);
        
        // create flatMap of obs period for all selected procedures
        Stream<ObsCluster> obsPeriods = procIds.flatMap(clusterStreamSupplier);
        
        // filter with FOIs id specified
        if (foiIDs != null)
            return obsPeriods.filter(p -> foiIDs.contains(p.getFoiID())).limit(filter.getLimit());
        else
            return obsPeriods.limit(filter.getLimit());
    }
    
    
    ObsKey getFirstObsPeriod(String procedureID, Instant start)
    {
        ObsKey beforeAll = ObsKey.builder()
                .withProcedureID(procedureID)
                .withPhenomenonTime(start)
                .build();
        return obsClustersIndex.ceilingKey(beforeAll);
    }
    
    
    ObsKey getLastObsPeriod(String procedureID, Instant end)
    {
        ObsKey afterAll = ObsKey.builder()
                .withProcedureID(procedureID)
                .withPhenomenonTime(end)
                .build();
        return obsClustersIndex.floorKey(afterAll);
    }


    @Override
    public Range<Instant> getPhenomenonTimeRange()
    {
        if (obsClustersIndex.isEmpty())
            return null;
        
        ObsKey first = getFirstObsPeriod(ObsCluster.ALL_PROCEDURES, LOWEST_TIME_KEY);
        ObsKey last = getLastObsPeriod(ObsCluster.ALL_PROCEDURES, HIGHEST_TIME_KEY);        
        return Range.closed(first.getPhenomenonTime(), last.getPhenomenonTime());
    }


    @Override
    public Duration getPhenomenonTimeStep()
    {
        return dataStoreInfo.samplingPeriod;
    }


    @Override
    public Stream<ObsCluster> getObsClustersByPhenomenonTime(ObsClusterFilter filter)
    {
        return getObsPeriodsGroupedByProcedure(filter, procID -> {
            // restrict to selected phenomenon time period only
            ObsKey first = getFirstObsPeriod(procID, filter.getTimeRange().getMin());
            ObsKey last = getLastObsPeriod(procID, filter.getTimeRange().getMax());
            RangeCursor<ObsKey, ObsCluster> cursor = new RangeCursor<>(obsClustersIndex, first, last);
            return StreamSupport.stream(cursor.valueIterator(), false);
        });
    }
    
    
    ObsKey getFirstResultPeriod(String procedureID, Instant start)
    {
        ObsKey beforeAll = ObsKey.builder()
                .withProcedureID(procedureID)
                .withPhenomenonTime(ALL_TIMES_KEY)
                .withResultTime(start)
                .build();
        return obsClustersIndex.ceilingKey(beforeAll);
    }
    
    
    ObsKey getLastResultPeriod(String procedureID, Instant end)
    {
        ObsKey afterAll = ObsKey.builder()
                .withProcedureID(procedureID)
                .withPhenomenonTime(ALL_TIMES_KEY)
                .withResultTime(end)
                .build();
        return obsClustersIndex.floorKey(afterAll);
    }


    @Override
    public Range<Instant> getResultTimeRange()
    {
        if (obsClustersIndex.isEmpty())
            return null;
        
        ObsKey first = getFirstResultPeriod(ObsCluster.ALL_PROCEDURES, LOWEST_TIME_KEY);
        ObsKey last = getLastResultPeriod(ObsCluster.ALL_PROCEDURES, HIGHEST_TIME_KEY);        
        return Range.closed(first.getResultTime(), last.getResultTime());
    }


    @Override
    public Duration getResultTimeStep()
    {
        return dataStoreInfo.resultPeriod;
    }


    @Override
    public Stream<ObsCluster> getObsClustersByResultTime(ObsClusterFilter filter)
    {
        return getObsPeriodsGroupedByProcedure(filter, procID -> {
            // restrict to selected result time period only
            ObsKey first = getFirstResultPeriod(procID, filter.getTimeRange().getMin());
            ObsKey last = getLastResultPeriod(procID, filter.getTimeRange().getMax());
            RangeCursor<ObsKey, ObsCluster> cursor = new RangeCursor<>(obsClustersIndex, first, last);
            return StreamSupport.stream(cursor.valueIterator(), false);
        });
    }
    
    
    RangeCursor<ObsKey, ObsData> getObsCursor(String procedureID, RangeFilter<Instant> phenomenonTime, RangeFilter<Instant> resultTime)
    {
        ObsKey first = ObsKey.builder()
                             .withProcedureID(procedureID)
                             .withResultTime(resultTime.getMin())
                             .withPhenomenonTime(phenomenonTime.getMin())
                             .build();
        
        ObsKey last = ObsKey.builder()
                            .withProcedureID(procedureID)
                            .withResultTime(resultTime.getMax())
                            .withPhenomenonTime(phenomenonTime.getMax())
                            .build();
        
        return new RangeCursor<>(obsRecordsIndex, first, last);
    }
    
    
    RangeCursor<ObsKey, ObsData> getObsCursor(String procedureID, Range<Instant> phenomenonTime, RangeFilter<Instant> resultTime)
    {
        ObsKey first = ObsKey.builder()
                             .withProcedureID(procedureID)
                             .withResultTime(resultTime.getMin())
                             .withPhenomenonTime(phenomenonTime.lowerEndpoint())
                             .build();
        
        ObsKey last = ObsKey.builder()
                            .withProcedureID(procedureID)
                            .withResultTime(resultTime.getMax())
                            .withPhenomenonTime(phenomenonTime.upperEndpoint())
                            .build();
        
        return new RangeCursor<>(obsRecordsIndex, first, last);
    }
    
    
    RangeCursor<FeatureKey, FoiPeriod> getFoiTimesCursor(String foiID, RangeFilter<Instant> timeRange)
    {
        FeatureKey first = new FeatureKey.Builder()
                .withUniqueID(foiID)
                .withValidStartTime(timeRange.getMin())
                .build();
        
        FeatureKey last = new FeatureKey.Builder()
                .withUniqueID(foiID)
                .withValidStartTime(timeRange.getMax())
                .build();
        
        // make sure we include first intersected period
        first = foiTimesIndex.floorKey(first);
        
        return new RangeCursor<>(foiTimesIndex, first, last);
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
        
        // if procedure JOIN, create Set of selected procedure IDs
        final Set<String> procIDs;
        if (filter.getProcedures() != null && procedureStore != null)
        {
            procIDs = procedureStore.selectKeys(filter.getProcedures())
                                    .map(k -> k.getUniqueID())
                                    .collect(Collectors.toSet());
            
            if (procIDs.isEmpty())
                return Stream.empty();
        }
        else
            procIDs = procedureStore.getAllFeatureIDs()
                                    .collect(Collectors.toSet());
        
        // if foi JOIN, prefetch selected foi IDs if needed
        // and build procedure ID to FOI periods map
        final Map<String, List<FoiPeriod>> procToFoisMap;
        if (filter.getFeaturesOfInterest() != null && foiStore != null)
        {
            Stream<String> foiIDs = foiStore.selectKeys(filter.getFeaturesOfInterest())
                                            .map(k -> k.getUniqueID());
            
            // stream periods for each FOI
            Stream<FoiPeriod> foiPeriods = foiIDs.flatMap(foiID -> {
                return StreamSupport.stream(
                    getFoiTimesCursor(foiID, phenomenonTimeFilter).valueIterator(),
                    false
                );
            });
            
            // filter with selected procIDs
            if (procIDs != null)
                foiPeriods = foiPeriods.filter(period -> procIDs.contains(period.getProcedureID()));
            
            // collect and group by procedure ID
            procToFoisMap = foiPeriods.collect(
                    Collectors.<FoiPeriod, String>groupingBy(period -> period.getProcedureID()));
            
            if (procToFoisMap.isEmpty())
                return Stream.empty();
        }
        else
            procToFoisMap = null;
        
        // create obs streams for each selected procedure
        final ArrayList<Spliterator<Entry<ObsKey, ObsData>>> obsIterators;
        if (procToFoisMap != null)
        {
            obsIterators = new ArrayList<>(procToFoisMap.size());
            procToFoisMap.forEach((procID, foiPeriods) -> {
                for (FoiPeriod period: foiPeriods) {                    
                    RangeCursor<ObsKey, ObsData> cursor = getObsCursor(procID, period.getPhenomenonTimeRange(), resultTimeFilter);
                    obsIterators.add(cursor.entryIterator());
                }
            });
        }
        else
        {
            obsIterators = new ArrayList<>(procIDs.size());
            procIDs.forEach((procID) -> {
                RangeCursor<ObsKey, ObsData> cursor = getObsCursor(procID, phenomenonTimeFilter, resultTimeFilter);
                obsIterators.add(cursor.entryIterator());
            });
        }
        
        // stream and merge obs from all selected procedures and time periods
        MergeSortSpliterator<Entry<ObsKey, ObsData>> mergeSortIt = new MergeSortSpliterator<>(obsIterators,
                (e1, e2) -> e1.getKey().getPhenomenonTime().compareTo(e2.getKey().getPhenomenonTime()));
                
        return StreamSupport.stream(mergeSortIt, false);
    }


    @Override
    public Stream<ObsData> select(ObsFilter filter)
    {
        return selectEntries(filter).map(e -> e.getValue());
    }


    @Override
    public Stream<DataBlock> selectResults(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Stream<ObsKey> selectKeys(ObsFilter filter)
    {
        return selectEntries(filter).map(e -> e.getKey());
    }


    @Override
    public Stream<ObsKey> removeEntries(ObsFilter filter)
    {
        // TODO optimize this?
        return selectKeys(filter).peek(k -> remove(k));
    }


    @Override
    public long countMatchingEntries(ObsFilter filter, long maxCount)
    {
        // TODO implement faster method for some special cases
        // i.e. when no predicates are used
        // can make use of H2 index counting feature
        
        return selectEntries(filter).limit(maxCount).count();
    }


    @Override
    public void clear()
    {
        obsRecordsIndex.clear();
        obsClustersIndex.clear();
        foiTimesIndex.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        return obsRecordsIndex.containsKey(key);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return obsRecordsIndex.containsValue(value);
    }


    @Override
    public Set<Entry<ObsKey, ObsData>> entrySet()
    {
        return obsRecordsIndex.entrySet();
    }


    @Override
    public ObsData get(Object key)
    {
        return obsRecordsIndex.get(key);
    }


    @Override
    public boolean isEmpty()
    {
        return obsRecordsIndex.isEmpty();
    }


    @Override
    public Set<ObsKey> keySet()
    {
        return obsRecordsIndex.keySet();
    }


    @Override
    public synchronized ObsData put(ObsKey key, ObsData value)
    {
        ObsData oldObs = obsRecordsIndex.put(key, value);
        updateObsPeriods(key);
        return oldObs;
    }
    
    
    private void updateObsPeriods(ObsKey key)
    {
        // group procedure (=ALL)
        // - last phenomenon time cluster
        // - last result time cluster
        // - spatial clusters
        
        // member procedure
        // - last phenomenon time cluster
        // - last result time cluster
        
    }


    @Override
    public synchronized void putAll(Map<? extends ObsKey, ? extends ObsData> map)
    {
        map.forEach((k, v) -> put(k, v));
    }


    @Override
    public ObsData remove(Object key)
    {
        ObsData oldObs = obsRecordsIndex.remove(key);
        
        // TODO update other indexes
        
        return oldObs;
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
    public boolean sync()
    {
        obsRecordsIndex.getStore().sync();
        return true;
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
