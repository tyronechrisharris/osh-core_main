/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RangeCursor;
import org.h2.mvstore.WriteBuffer;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStats;
import org.sensorhub.api.datastore.command.CommandStatsQuery;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.MergeSortSpliterator;
import org.sensorhub.impl.datastore.h2.MVDatabaseConfig.IdProviderType;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;


/**
 * <p>
 * Implementation of command store based on H2 MVStore.
 * </p><p>
 * Several instances of this store can be contained in the same MVStore
 * as long as they have different names.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 25, 2021
 */
public class MVCommandStoreImpl implements ICommandStore
{
    private static final String CMD_RECORDS_MAP_NAME = "cmd_records";
    private static final String CMD_SERIES_MAP_NAME = "cmd_series";
    
    protected MVStore mvStore;
    protected MVDataStoreInfo dataStoreInfo;
    protected MVCommandStreamStoreImpl cmdStreamStore;
    protected MVCommandStatusStoreImpl cmdStatusStore;
    protected MVBTreeMap<MVTimeSeriesRecordKey, ICommandData> cmdRecordsIndex;
    protected MVBTreeMap<MVTimeSeriesKey, MVTimeSeriesInfo> cmdSeriesMainIndex;
    
    protected IFoiStore foiStore;
    protected int maxSelectedSeriesOnJoin = 200;
    
    
    static class TimeParams
    {
        Range<Instant> issueTimeRange;
        boolean currentTimeOnly;
        boolean latestResultOnly;
        
        
        TimeParams(CommandFilter filter)
        {
            // get issue time range
            issueTimeRange = filter.getIssueTime() != null ?
                filter.getIssueTime().getRange() : H2Utils.ALL_TIMES_RANGE;
                
            latestResultOnly = filter.getIssueTime() != null && filter.getIssueTime().isLatestTime();
            currentTimeOnly = filter.getIssueTime() != null && filter.getIssueTime().isCurrentTime();
        }
    }
    
    
    private MVCommandStoreImpl()
    {
    }


    /**
     * Opens an existing command store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param systemStore associated system descriptions data store
     * @param idProviderType Type of ID provider to use to generate new command stream IDs
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVCommandStoreImpl open(MVStore mvStore, IdProviderType idProviderType, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        // create ID provider
        IdProvider<ICommandStreamInfo> idProvider = null;
        if (idProviderType == IdProviderType.UID_HASH)
        {
            var hashFunc = Hashing.murmur3_128(741532149);
            idProvider = dsInfo -> {
                var hasher = hashFunc.newHasher();
                hasher.putLong(dsInfo.getSystemID().getInternalID());
                hasher.putUnencodedChars(dsInfo.getControlInputName());
                hasher.putLong(dsInfo.getValidTime().begin().toEpochMilli());
                return hasher.hash().asLong() & 0xFFFFFFFFFFFFL; // keep only 48 bits
            };
        }
        
        return new MVCommandStoreImpl().init(mvStore, dataStoreInfo, idProvider);
    }
    
    
    private MVCommandStoreImpl init(MVStore mvStore, MVDataStoreInfo dataStoreInfo, IdProvider<ICommandStreamInfo> dsIdProvider)
    {
        this.mvStore = Asserts.checkNotNull(mvStore, MVStore.class);
        this.dataStoreInfo = Asserts.checkNotNull(dataStoreInfo, MVDataStoreInfo.class);
        this.cmdStreamStore = new MVCommandStreamStoreImpl(this, dsIdProvider);
        this.cmdStatusStore = new MVCommandStatusStoreImpl(this);
        
        // persistent class mappings for Kryo
        var kryoClassMap = mvStore.openMap(MVObsSystemDatabase.KRYO_CLASS_MAP_NAME, new MVBTreeMap.Builder<String, Integer>());
        
        // commands map
        String mapName = dataStoreInfo.getName() + ":" + CMD_RECORDS_MAP_NAME;
        this.cmdRecordsIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVTimeSeriesRecordKey, ICommandData>()
                .keyType(new MVTimeSeriesRecordKeyDataType())
                .valueType(new CommandDataType(kryoClassMap)));
        
        // commands series map
        mapName = dataStoreInfo.getName() + ":" + CMD_SERIES_MAP_NAME;
        this.cmdSeriesMainIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVTimeSeriesKey, MVTimeSeriesInfo>()
                .keyType(new MVCommandSeriesKeyByCommandStreamDataType())
                .valueType(new MVTimeSeriesInfoDataType()));
        
        return this;
    }


    @Override
    public String getDatastoreName()
    {
        return dataStoreInfo.getName();
    }


    @Override
    public long getNumRecords()
    {
        return cmdRecordsIndex.sizeAsLong();
    }
    
    
    Stream<MVTimeSeriesInfo> getAllCommandSeries()
    {
        MVTimeSeriesKey first = new MVTimeSeriesKey(0, 0);
        MVTimeSeriesKey last = new MVTimeSeriesKey(Long.MAX_VALUE, Long.MAX_VALUE);
        RangeCursor<MVTimeSeriesKey, MVTimeSeriesInfo> cursor = new RangeCursor<>(cmdSeriesMainIndex, first, last);
        
        return cursor.entryStream()
            .map(e -> {
                e.getValue().key = e.getKey();
                return e.getValue();
            });
    }
    
    
    Stream<MVTimeSeriesInfo> getCommandSeriesByDataStream(long cmdStreamID)
    {
        // scan series for all receivers of the selected system
        MVTimeSeriesKey first = new MVTimeSeriesKey(cmdStreamID, 0);
        MVTimeSeriesKey last = new MVTimeSeriesKey(cmdStreamID, Long.MAX_VALUE);
        RangeCursor<MVTimeSeriesKey, MVTimeSeriesInfo> cursor = new RangeCursor<>(cmdSeriesMainIndex, first, last);
        
        return cursor.entryStream()
            .map(e -> {
                MVTimeSeriesInfo series = e.getValue();
                series.key = e.getKey();
                return series;
            });
    }
    
    
    RangeCursor<MVTimeSeriesRecordKey, ICommandData> getCommandCursor(long seriesID, Range<Instant> issueTimeRange)
    {
        MVTimeSeriesRecordKey first = new MVTimeSeriesRecordKey(seriesID, issueTimeRange.lowerEndpoint());
        MVTimeSeriesRecordKey last = new MVTimeSeriesRecordKey(seriesID, issueTimeRange.upperEndpoint());
        return new RangeCursor<>(cmdRecordsIndex, first, last);
    }
    
    
    Stream<Entry<MVTimeSeriesRecordKey, ICommandData>> getCommandStream(MVTimeSeriesInfo series, Range<Instant> issueTimeRange, boolean latestOnly)
    {        
        // if request if for latest only, get only the latest command in series
        if (latestOnly)
        {
            MVTimeSeriesRecordKey maxKey = new MVTimeSeriesRecordKey(series.id, Instant.MAX);
            Entry<MVTimeSeriesRecordKey, ICommandData> e = cmdRecordsIndex.floorEntry(maxKey);
            if (e != null && e.getKey().seriesID == series.id)
                return Stream.of(e);
            else
                return Stream.empty();
        }
        
        // scan using a cursor on main command index
        // recreating full entries in the process
        RangeCursor<MVTimeSeriesRecordKey, ICommandData> cursor = getCommandCursor(series.id, issueTimeRange);
        return cursor.entryStream();
    }
    
    
    BigInteger mapToPublicKey(MVTimeSeriesRecordKey internalKey)
    {
        // compute internal ID
        WriteBuffer buf = new WriteBuffer(24); // seriesID + timestamp seconds + nanos
        DataUtils.writeVarLong(buf.getBuffer(), internalKey.getSeriesID());
        H2Utils.writeInstant(buf, internalKey.getTimeStamp());
        return new BigInteger(buf.getBuffer().array(), 0, buf.position());
    }
    
    
    MVTimeSeriesRecordKey mapToInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger, "key must be a BigInteger");
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            ByteBuffer buf = ByteBuffer.wrap(key.toByteArray());
            long seriesID = DataUtils.readVarLong(buf);
            Instant issueTime = H2Utils.readInstant(buf);
            
            return new MVTimeSeriesRecordKey(seriesID, issueTime);
        }
        catch (Exception e)
        {
            // invalid bigint key
            return null;
        }
    }
    
    
    Entry<BigInteger, ICommandData> mapToPublicEntry(Entry<MVTimeSeriesRecordKey, ICommandData> internalEntry)
    {
        BigInteger cmdID = mapToPublicKey(internalEntry.getKey());
        return new DataUtils.MapEntry<>(cmdID, internalEntry.getValue());
    }
    
    
    /*
     * Select all command series matching the filter
     */
    protected Stream<MVTimeSeriesInfo> selectCommandSeries(CommandFilter filter)
    {
        // otherwise prepare stream of matching command series
        Stream<MVTimeSeriesInfo> cmdSeries = null;
        
        // if no command stream nor FOI filter used, scan all commands
        if (filter.getCommandStreamFilter() == null)
        {
            cmdSeries = getAllCommandSeries();
        }
        
        // only command stream filter used
        else
        {
            // stream directly from list of selected datastreams
            cmdSeries = DataStoreUtils.selectCommandStreamIDs(cmdStreamStore, filter.getCommandStreamFilter())
                .flatMap(id -> getCommandSeriesByDataStream(id));
        }
        
        return cmdSeries;
    }


    @Override
    public Stream<Entry<BigInteger, ICommandData>> selectEntries(CommandFilter filter, Set<CommandField> fields)
    {        
        // stream command directly in case of filtering by internal IDs
        if (filter.getInternalIDs() != null)
        {
            var cmdStream = filter.getInternalIDs().stream()
                .map(k -> mapToInternalKey(k))
                .map(k -> cmdRecordsIndex.getEntry(k));
            
            return getPostFilteredResultStream(cmdStream, filter)
                .map(this::mapToPublicEntry);
        }
        
        // select command series matching the filter
        var timeParams = new TimeParams(filter);
        var cmdSeries = selectCommandSeries(filter);
        
        // create command streams for each selected series
        // and keep all spliterators in array list
        final var cmdStreams = new ArrayList<Stream<Entry<MVTimeSeriesRecordKey, ICommandData>>>(100);
        cmdStreams.add(cmdSeries
            .peek(s -> {
                // make sure list size cannot go over a threshold
                if (cmdStreams.size() >= maxSelectedSeriesOnJoin)
                    throw new IllegalStateException("Too many command streams or command receivers selected. Please refine your filter");
            })
            .flatMap(series -> {
                var cmdStream = getCommandStream(series, timeParams.issueTimeRange,
                    timeParams.currentTimeOnly || timeParams.latestResultOnly);
                return getPostFilteredResultStream(cmdStream, filter);
            }));
        
        
        // stream and merge commands from all selected command streams and time periods
        var mergeSortIt = new MergeSortSpliterator<Entry<MVTimeSeriesRecordKey, ICommandData>>(cmdStreams,
                (e1, e2) -> e1.getValue().getIssueTime().compareTo(e2.getValue().getIssueTime()));
               
        // stream output of merge sort iterator + apply limit
        return StreamSupport.stream(mergeSortIt, false)
            .map(this::mapToPublicEntry)
            .limit(filter.getLimit())
            .onClose(() -> mergeSortIt.close());
    }
    
    
    Stream<Entry<MVTimeSeriesRecordKey, ICommandData>> getPostFilteredResultStream(Stream<Entry<MVTimeSeriesRecordKey, ICommandData>> resultStream, CommandFilter filter)
    {
        // first test with command filter
        resultStream = resultStream.filter(e -> filter.test(e.getValue()));
        
        // also filter on status
        if (filter.getStatusFilter() != null)
        {
            var statusFilter = filter.getStatusFilter();
            resultStream = resultStream.filter(cmd -> {
                return cmdStatusStore.getStatusStream(mapToPublicKey(cmd.getKey()), statusFilter.getReportTime())
                    .anyMatch(status -> statusFilter.test(status.getValue()));
            });
        }
        
        return resultStream;
    }
        
    
    TimeExtent getCommandStreamActuationTimeRange(long dataStreamID)
    {
        // for now just return issue time range
        return getCommandStreamIssueTimeRange(dataStreamID);
    }
    
    
    TimeExtent getCommandStreamIssueTimeRange(long dataStreamID)
    {
        Instant[] timeRange = new Instant[] {Instant.MAX, Instant.MIN};
        getCommandSeriesByDataStream(dataStreamID)
            .forEach(s -> {
                var seriesTimeRange = getCommandSeriesIssueTimeRange(s.id);
                if (seriesTimeRange == null)
                    return;
                
                if (timeRange[0].isAfter(seriesTimeRange.lowerEndpoint()))
                    timeRange[0] = seriesTimeRange.lowerEndpoint();
                if (timeRange[1].isBefore(seriesTimeRange.upperEndpoint()))
                    timeRange[1] = seriesTimeRange.upperEndpoint();
            });
        
        if (timeRange[0] == Instant.MAX || timeRange[1] == Instant.MIN)
            return null;
        else
            return TimeExtent.period(timeRange[0], timeRange[1]);
    }
    
    
    Range<Instant> getCommandSeriesIssueTimeRange(long seriesID)
    {
        MVTimeSeriesRecordKey firstKey = cmdRecordsIndex.ceilingKey(new MVTimeSeriesRecordKey(seriesID, Instant.MIN));
        MVTimeSeriesRecordKey lastKey = cmdRecordsIndex.floorKey(new MVTimeSeriesRecordKey(seriesID, Instant.MAX));
        
        if (firstKey == null || lastKey == null ||
            firstKey.seriesID != seriesID || lastKey.seriesID != seriesID)
            return null;
        else
            return Range.closed(firstKey.timeStamp, lastKey.timeStamp);
    }
    
    
    long getCommandSeriesCount(long seriesID, Range<Instant> issueTimeRange)
    {
        MVTimeSeriesRecordKey firstKey = cmdRecordsIndex.ceilingKey(new MVTimeSeriesRecordKey(seriesID, issueTimeRange.lowerEndpoint()));
        MVTimeSeriesRecordKey lastKey = cmdRecordsIndex.floorKey(new MVTimeSeriesRecordKey(seriesID, issueTimeRange.upperEndpoint()));
        
        if (firstKey == null || lastKey == null ||
            firstKey.seriesID != seriesID || lastKey.seriesID != seriesID)
            return 0;
        else
            return cmdRecordsIndex.getKeyIndex(lastKey) - cmdRecordsIndex.getKeyIndex(firstKey) + 1;
    }
    
    
    int[] getCommandSeriesHistogram(long seriesID, Range<Instant> issueTimeRange, Duration binSize)
    {
        long start = issueTimeRange.lowerEndpoint().getEpochSecond();
        long end = issueTimeRange.upperEndpoint().getEpochSecond();
        long dt = binSize.getSeconds();
        long t = start;
        int numBins = (int)((end - start)/dt);
        int[] counts = new int[numBins];
        
        for (int i = 0; i < counts.length; i++)
        {
            var beginBin = Instant.ofEpochSecond(t);
            MVTimeSeriesRecordKey k1 = cmdRecordsIndex.ceilingKey(new MVTimeSeriesRecordKey(seriesID, beginBin));
            
            t += dt;
            var endBin = Instant.ofEpochSecond(t);
            MVTimeSeriesRecordKey k2 = cmdRecordsIndex.floorKey(new MVTimeSeriesRecordKey(seriesID, endBin));
            
            if (k1 != null && k2 != null && k1.seriesID == seriesID && k2.seriesID == seriesID)
            {
                long idx1 = cmdRecordsIndex.getKeyIndex(k1);
                long idx2 = cmdRecordsIndex.getKeyIndex(k2);
                
                // only compute count if key2 is after key1
                // otherwise it means there was no matching key inside this bin
                if (idx2 >= idx1)
                {
                    int count = (int)(idx2-idx1);
                    
                    // need to add one unless end of bin falls exactly on a key 
                    if (!endBin.equals(k2.timeStamp))
                        count++;
                    
                    counts[i] = count;
                }
            }
        }
        
        return counts;
    }


    @Override
    public Stream<CommandStats> getStatistics(CommandStatsQuery query)
    {
        var filter = query.getCommandFilter();
        var timeParams = new TimeParams(filter);
        
        /*if (query.isAggregateFois())
        {
            return null;
        }
        else*/
        {
            return selectCommandSeries(filter)
                .map(series -> {
                   var dsID = series.key.dataStreamID;
                   
                   var seriesTimeRange = getCommandSeriesIssueTimeRange(series.id);
                   
                   // skip if requested issue time range doesn't intersect series time range
                   var statsTimeRange = timeParams.issueTimeRange;
                   if (!statsTimeRange.isConnected(seriesTimeRange))
                       return null;
                   
                   statsTimeRange = seriesTimeRange.intersection(seriesTimeRange);
                   
                   var issueTimeRange = series.key.resultTime != Instant.MIN ?
                       Range.singleton(series.key.resultTime) : statsTimeRange;
                   
                   var cmdCount = getCommandSeriesCount(series.id, statsTimeRange);
                       
                   var cmdStats = new CommandStats.Builder()
                       .withCommandStreamID(dsID)
                       .withActuationTimeRange(TimeExtent.period(statsTimeRange))
                       .withIssueTimeRange(TimeExtent.period(issueTimeRange))
                       .withTotalCommandCount(cmdCount);
                   
                   // compute histogram
                   if (query.getHistogramBinSize() != null)
                   {
                       var histogramTimeRange = timeParams.issueTimeRange;
                       if (histogramTimeRange.lowerEndpoint() == Instant.MIN || histogramTimeRange.upperEndpoint() == Instant.MAX)
                           histogramTimeRange = seriesTimeRange;
                       
                       cmdStats.withCommandCountByTime(getCommandSeriesHistogram(series.id,
                           histogramTimeRange, query.getHistogramBinSize()));
                   }
                   
                   return cmdStats.build();
                })
                .filter(Objects::nonNull);
        }
    }


    @Override
    public long countMatchingEntries(CommandFilter filter)
    {
        var timeParams = new TimeParams(filter);
        
        // if no predicate or spatial query is used, we can optimize
        // by scanning only command series
        if (filter.getValuePredicate() == null)
        {
            // special case to count per series
            return selectCommandSeries(filter)
                .mapToLong(series -> {
                    return getCommandSeriesCount(series.id, timeParams.issueTimeRange);
                })
                .sum();
        }
        
        // else use full select and count items
        else
            return selectKeys(filter).limit(filter.getLimit()).count();
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
                cmdRecordsIndex.clear();
                cmdSeriesMainIndex.clear();
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
        MVTimeSeriesRecordKey cmdKey = mapToInternalKey(key);
        return cmdKey == null ? false : cmdRecordsIndex.containsKey(cmdKey);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return cmdRecordsIndex.containsValue(value);
    }


    @Override
    public ICommandData get(Object key)
    {
        MVTimeSeriesRecordKey cmdKey = mapToInternalKey(key);
        return cmdKey == null ? null : cmdRecordsIndex.get(cmdKey);
    }


    @Override
    public boolean isEmpty()
    {
        return cmdRecordsIndex.isEmpty();
    }


    @Override
    public Set<Entry<BigInteger, ICommandData>> entrySet()
    {
        return new AbstractSet<>() {        
            @Override
            public Iterator<Entry<BigInteger, ICommandData>> iterator() {
                return getAllCommandSeries()
                    .flatMap(series -> {
                        RangeCursor<MVTimeSeriesRecordKey, ICommandData> cursor = getCommandCursor(series.id, H2Utils.ALL_TIMES_RANGE);
                        return cursor.entryStream().map(e -> {
                            return mapToPublicEntry(e);
                        });
                    }).iterator();
            }

            @Override
            public int size() {
                return cmdRecordsIndex.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVCommandStoreImpl.this.containsKey(o);
            }
        };
    }


    @Override
    public Set<BigInteger> keySet()
    {
        return new AbstractSet<>() {
            @Override
            public Iterator<BigInteger> iterator() {
                return getAllCommandSeries()
                    .flatMap(series -> {
                        RangeCursor<MVTimeSeriesRecordKey, ICommandData> cursor = getCommandCursor(series.id, H2Utils.ALL_TIMES_RANGE);
                        return cursor.keyStream().map(e -> {
                            return mapToPublicKey(e);
                        });
                    }).iterator();
            }

            @Override
            public int size() {
                return cmdRecordsIndex.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVCommandStoreImpl.this.containsKey(o);
            }
        };
    }
    
    
    @Override
    public BigInteger add(ICommandData cmd)
    {
        // check that command stream exists
        if (!cmdStreamStore.containsKey(new CommandStreamKey(cmd.getCommandStreamID())))
            throw new IllegalStateException("Unknown command stream: " + cmd.getCommandStreamID()); 
            
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVTimeSeriesKey seriesKey = new MVTimeSeriesKey(cmd.getCommandStreamID(), cmd.getFoiID());
                
                MVTimeSeriesInfo series = cmdSeriesMainIndex.computeIfAbsent(seriesKey, k -> {
                    return new MVTimeSeriesInfo(
                        cmdRecordsIndex.isEmpty() ? 1 : cmdRecordsIndex.lastKey().seriesID + 1);
                });
                
                // add to main command index
                MVTimeSeriesRecordKey cmdKey = new MVTimeSeriesRecordKey(series.id, cmd.getIssueTime());
                cmdRecordsIndex.put(cmdKey, cmd);
                
                return mapToPublicKey(cmdKey);
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public ICommandData put(BigInteger key, ICommandData cmd)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVTimeSeriesRecordKey cmdKey = mapToInternalKey(key);
                ICommandData oldCmd = cmdRecordsIndex.replace(cmdKey, cmd);
                if (oldCmd == null)
                    throw new UnsupportedOperationException("put can only be used to update existing keys");
                return oldCmd;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public ICommandData remove(Object keyObj)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVTimeSeriesRecordKey key = mapToInternalKey(keyObj);
                ICommandData oldCmd = cmdRecordsIndex.remove(key);
                
                // don't check and remove empty command series here since in many cases they will be reused.
                // it can be done automatically during cleanup/compaction phase or with specific method.
                
                return oldCmd;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }        
    }
    

    protected void removeAllCommandsAndSeries(long commandStreamID)
    {
        // remove all series and commands
        MVTimeSeriesKey first = new MVTimeSeriesKey(commandStreamID, 0, Instant.MIN);
        MVTimeSeriesKey last = new MVTimeSeriesKey(commandStreamID, Long.MAX_VALUE, Instant.MAX);
        
        new RangeCursor<>(cmdSeriesMainIndex, first, last).entryStream().forEach(entry -> {

            // remove all commands in series
            var seriesId = entry.getValue().id;
            MVTimeSeriesRecordKey k1 = new MVTimeSeriesRecordKey(seriesId, Instant.MIN);
            MVTimeSeriesRecordKey k2 = new MVTimeSeriesRecordKey(seriesId, Instant.MAX);
            new RangeCursor<>(cmdRecordsIndex, k1, k2).keyStream().forEach(k -> {
                cmdRecordsIndex.remove(k);
                
                // remove all status reports
                cmdStatusStore.removeAllStatus(mapToPublicKey(k));
            });
            
            // remove series from index
            cmdSeriesMainIndex.remove(entry.getKey());
        });
    }


    @Override
    public int size()
    {
        return cmdRecordsIndex.size();
    }


    @Override
    public Collection<ICommandData> values()
    {
        return cmdRecordsIndex.values();
    }


    @Override
    public void commit()
    {
        cmdRecordsIndex.getStore().commit();
        cmdRecordsIndex.getStore().sync();
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
    public boolean isReadOnly()
    {
        return mvStore.isReadOnly();
    }
    

    @Override
    public ICommandStreamStore getCommandStreams()
    {
        return cmdStreamStore;
    }


    @Override
    public ICommandStatusStore getStatusReports()
    {
        return cmdStatusStore;
    }
    
    
    @Override
    public void linkTo(IFoiStore foiStore)
    {
        this.foiStore = Asserts.checkNotNull(foiStore, IFoiStore.class);
    }
}
