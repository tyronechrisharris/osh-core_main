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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.RangeCursor;
import org.h2.mvstore.WriteBuffer;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStatusFilter;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.collect.Range;


/**
 * <p>
 * Implementation of command status store based on H2 MVStore.
 * </p><p>
 * Several instances of this store can be contained in the same MVStore
 * as long as they have different names.
 * </p>
 *
 * @author Alex Robin
 * @date Jan 5, 2022
 */
public class MVCommandStatusStoreImpl implements ICommandStatusStore
{
    private static final String CMD_RECORDS_MAP_NAME = "status_records";
    
    protected MVStore mvStore;
    protected MVCommandStoreImpl cmdStore;
    protected IdProvider<ICommandStreamInfo> idProvider;
    
    protected MVBTreeMap<MVCommandStatusKey, ICommandStatus> statusIndex;
    
    protected int maxSelectedSeriesOnJoin = 200;
    
    
    static class TimeParams
    {
        Range<Instant> reportTimeRange;
        Range<Instant> execTimeRange;
        boolean currentTimeOnly;
        boolean latestResultOnly;
        
        
        TimeParams(CommandStatusFilter filter)
        {
            // get report time range
            reportTimeRange = filter.getReportTime() != null ?
                filter.getReportTime().getRange() : H2Utils.ALL_TIMES_RANGE;
            
            // get execution time range
            execTimeRange = filter.getExecutionTime() != null ?
                filter.getExecutionTime().getRange() : H2Utils.ALL_TIMES_RANGE;
            
            // try to derive execution time range from report time range
            // so we can use the time index 
            if (filter.getReportTime() == null && execTimeRange != null && execTimeRange != H2Utils.ALL_TIMES_RANGE)
            {
                var begin = execTimeRange.lowerEndpoint().minus(1, ChronoUnit.DAYS);
                var end = execTimeRange.upperEndpoint();
                reportTimeRange = Range.closed(begin, end);
            }
            
            latestResultOnly = filter.getReportTime() != null && filter.getReportTime().isLatestTime();
            currentTimeOnly = filter.getExecutionTime() != null && filter.getExecutionTime().isCurrentTime();
        }
    }
    
    
    protected MVCommandStatusStoreImpl(MVCommandStoreImpl cmdStore)
    {
        this.cmdStore = Asserts.checkNotNull(cmdStore, MVCommandStoreImpl.class);
        this.mvStore = Asserts.checkNotNull(cmdStore.mvStore, MVStore.class);
        
        // persistent class mappings for Kryo
        var kryoClassMap = mvStore.openMap(MVObsSystemDatabase.KRYO_CLASS_MAP_NAME, new MVBTreeMap.Builder<String, Integer>());
        
        // commands map
        String mapName = cmdStore.getDatastoreName() + ":" + CMD_RECORDS_MAP_NAME;
        this.statusIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVCommandStatusKey, ICommandStatus>()
                .keyType(new MVCommandStatusKeyDataType())
                .valueType(new CommandStatusDataType(kryoClassMap)));
    }


    @Override
    public String getDatastoreName()
    {
        return cmdStore.getDatastoreName();
    }


    @Override
    public long getNumRecords()
    {
        return statusIndex.sizeAsLong();
    }
    
    
    BigInteger toExternalKey(MVCommandStatusKey internalKey)
    {
        byte[] cmdID = internalKey.cmdID.toByteArray();
        WriteBuffer buf = new WriteBuffer(cmdID.length + 13); // 1+8+4
        buf.put((byte)cmdID.length);
        buf.put(cmdID);
        H2Utils.writeInstant(buf, internalKey.reportTime);
        return new BigInteger(buf.getBuffer().array(), 0, buf.position());
    }
    
    
    MVCommandStatusKey toInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger, "key must be a BigInteger");
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            ByteBuffer buf = ByteBuffer.wrap(key.toByteArray());
            int cmdIdLen = buf.get();
            BigInteger cmdID = new BigInteger(buf.array(), 1, cmdIdLen);
            buf.position(cmdIdLen+1);
            Instant reportTime = H2Utils.readInstant(buf);
            return new MVCommandStatusKey(cmdID, reportTime);
        }
        catch (Exception e)
        {
            // invalid bigint key
            return null;
        }
    }
    
    
    Entry<BigInteger, ICommandStatus> toExternalEntry(Entry<MVCommandStatusKey, ICommandStatus> internalEntry)
    {
        BigInteger cmdID = toExternalKey(internalEntry.getKey());
        return new DataUtils.MapEntry<>(cmdID, internalEntry.getValue());
    }
    
    
    RangeCursor<MVCommandStatusKey, ICommandStatus> getStatusCursor(BigInteger cmdID, Range<Instant> reportTimeRange)
    {
        MVCommandStatusKey first = new MVCommandStatusKey(cmdID, reportTimeRange.lowerEndpoint());
        MVCommandStatusKey last = new MVCommandStatusKey(cmdID, reportTimeRange.upperEndpoint());
        return new RangeCursor<>(statusIndex, first, last);
    }
    
    
    Stream<Entry<MVCommandStatusKey, ICommandStatus>> getStatusStream(BigInteger cmdID, TemporalFilter reportTimeFilter)
    {
        // if request if for latest only, get only the latest command in series
        if (reportTimeFilter != null && reportTimeFilter.isLatestTime())
        {
            MVCommandStatusKey maxKey = new MVCommandStatusKey(cmdID, Instant.MAX);
            Entry<MVCommandStatusKey, ICommandStatus> e = statusIndex.floorEntry(maxKey);
            if (e != null && e.getKey().cmdID.equals(cmdID))
                return Stream.of(e);
            else
                return Stream.empty();
        }
        
        // scan using a cursor on main command index
        var timeRange = reportTimeFilter != null ? reportTimeFilter.getRange() : H2Utils.ALL_TIMES_RANGE;
        RangeCursor<MVCommandStatusKey, ICommandStatus> cursor = getStatusCursor(cmdID, timeRange);
        return cursor.entryStream();
    }


    @Override
    public Stream<Entry<BigInteger, ICommandStatus>> selectEntries(CommandStatusFilter filter, Set<CommandStatusField> fields)
    {
        Stream<Entry<MVCommandStatusKey, ICommandStatus>> resultStream;
        var commandFilter = filter.getCommandFilter();
        
        if (commandFilter == null)
            commandFilter = cmdStore.selectAllFilter();
        
        // lookup status for each selected command
        resultStream = cmdStore.selectKeys(commandFilter)
            .flatMap(k -> getStatusStream(k, filter.getReportTime()));
        
        // post filter status record
        // creating public entries in the process
        return resultStream.filter(e -> filter.test(e.getValue()))
            .map(e -> toExternalEntry(e))
            .limit(filter.getLimit());
    }
    
    
    TimeExtent getCommandStreamReportTimeRange(long streamID)
    {
        // create filter to select all commands from stream
        var filter = new CommandFilter.Builder()
            .withCommandStreams(streamID)
            .build();
        
        Instant[] timeRange = new Instant[] {Instant.MAX, Instant.MIN};
        
        // get report time range for each command
        cmdStore.selectKeys(filter)
            .forEach(cmdID -> {
                
                // get first and last key for this command
                var minKey = new MVCommandStatusKey(cmdID, Instant.MIN);
                var firstKey = statusIndex.ceilingKey(minKey);
                var maxKey = new MVCommandStatusKey(cmdID, Instant.MAX);
                var lastKey = statusIndex.floorKey(maxKey);
                
                if (firstKey.cmdID == cmdID && lastKey.cmdID == cmdID)
                {
                    if (timeRange[0].isAfter(firstKey.reportTime))
                        timeRange[0] = firstKey.reportTime;
                    if (timeRange[1].isBefore(lastKey.reportTime))
                        timeRange[1] = lastKey.reportTime;
                }
            });
        
        if (timeRange[0] == Instant.MAX || timeRange[1] == Instant.MIN)
            return null;
        else
            return TimeExtent.period(timeRange[0], timeRange[1]);
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
                statusIndex.clear();
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
        MVCommandStatusKey cmdKey = toInternalKey(key);
        return cmdKey == null ? false : statusIndex.containsKey(cmdKey);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return statusIndex.containsValue(value);
    }


    @Override
    public ICommandStatus get(Object key)
    {
        MVCommandStatusKey cmdKey = toInternalKey(key);
        return cmdKey == null ? null : statusIndex.get(cmdKey);
    }


    @Override
    public boolean isEmpty()
    {
        return statusIndex.isEmpty();
    }


    @Override
    public Set<Entry<BigInteger, ICommandStatus>> entrySet()
    {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<BigInteger, ICommandStatus>> iterator() {
                return statusIndex.entrySet().stream()
                    .map(e -> toExternalEntry(e))
                    .iterator();
            }

            @Override
            public int size() {
                return statusIndex.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVCommandStatusStoreImpl.this.containsKey(o);
            }
        };
    }


    @Override
    public Set<BigInteger> keySet()
    {
        return new AbstractSet<>() {
            @Override
            public Iterator<BigInteger> iterator() {
                return statusIndex.keyStream()
                    .map(key -> toExternalKey(key))
                    .iterator();
            }

            @Override
            public int size() {
                return statusIndex.size();
            }

            @Override
            public boolean contains(Object o) {
                return MVCommandStatusStoreImpl.this.containsKey(o);
            }
        };
    }
    
    
    @Override
    public BigInteger add(ICommandStatus status)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                // add to main index
                MVCommandStatusKey key = new MVCommandStatusKey(
                    status.getCommandID(),
                    status.getReportTime());
                statusIndex.put(key, status);
                
                return toExternalKey(key);
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public ICommandStatus put(BigInteger key, ICommandStatus status)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVCommandStatusKey internalKey = toInternalKey(key);
                ICommandStatus oldStatus = statusIndex.replace(internalKey, status);
                if (oldStatus == null)
                    throw new UnsupportedOperationException("put can only be used to update existing keys");
                return oldStatus;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public ICommandStatus remove(Object keyObj)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                MVCommandStatusKey key = toInternalKey(keyObj);
                ICommandStatus oldCmd = statusIndex.remove(key);
                
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
    

    protected void removeAllStatus(BigInteger cmdID)
    {
        // remove all series and commands
        MVCommandStatusKey first = new MVCommandStatusKey(cmdID, Instant.MIN);
        MVCommandStatusKey last = new MVCommandStatusKey(cmdID, Instant.MAX);
        
        new RangeCursor<>(statusIndex, first, last).keyStream().forEach(k -> {
            statusIndex.remove(k);
        });
    }


    @Override
    public int size()
    {
        return statusIndex.size();
    }


    @Override
    public Collection<ICommandStatus> values()
    {
        return statusIndex.values();
    }


    @Override
    public void commit()
    {
        statusIndex.getStore().commit();
        statusIndex.getStore().sync();
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
}
