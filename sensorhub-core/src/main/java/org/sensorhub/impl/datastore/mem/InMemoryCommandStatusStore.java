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
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandStatusFilter;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * In-memory implementation of a command status store backed by a {@link NavigableMap}.
 * This implementation is only used to store the status reports for the
 * latest received command in each command stream.
 * </p>
 *
 * @author Alex Robin
 * @date Jan 4, 2022
 */
public class InMemoryCommandStatusStore extends InMemoryDataStore implements ICommandStatusStore
{
    static final TemporalFilter ALL_TIMES_FILTER = new TemporalFilter.Builder().withAllTimes().build();
    
    NavigableMap<StatusKey, ICommandStatus> map = new ConcurrentSkipListMap<>(new StatusKeyComparator());
    final InMemoryCommandStore cmdStore;
    
    
    static class StatusKey
    {    
        BigInteger cmdID;
        Instant reportTime = null;
        
        StatusKey(BigInteger cmdID, Instant reportTime)
        {
            this.cmdID = cmdID;
            this.reportTime = reportTime;
        }

        @Override
        public String toString()
        {
            return ObjectUtils.toString(this, true);
        }
    }
    
    
    private static class StatusKeyComparator implements Comparator<StatusKey>
    {
        @Override
        public int compare(StatusKey k1, StatusKey k2)
        {
            // first compare command IDs
            int comp = k1.cmdID.compareTo(k2.cmdID);
            if (comp != 0)
                return comp;
            
            // then compare report times
            return k1.reportTime.compareTo(k2.reportTime);
        }
    }
    
    
    public InMemoryCommandStatusStore(InMemoryCommandStore cmdStore)
    {
        this.cmdStore = Asserts.checkNotNull(cmdStore, InMemoryCommandStore.class);
    }
    
    
    BigInteger toExternalKey(StatusKey key)
    {
        byte[] cmdID = key.cmdID.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(cmdID.length + 13); // 1+8+4
        buf.put((byte)cmdID.length);
        buf.put(cmdID);
        buf.putInt((int)(key.reportTime.getEpochSecond()));
        buf.putInt(key.reportTime.getNano());
        return new BigInteger(buf.array(), 0, buf.position());
    }
    
    
    StatusKey toInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger);
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            ByteBuffer buf = ByteBuffer.wrap(key.toByteArray());
            int cmdIdLen = buf.get();
            BigInteger cmdID = new BigInteger(buf.array(), 1, cmdIdLen);
            buf.position(cmdIdLen+1);
            Instant reportTime = Instant.ofEpochSecond(buf.getInt(), buf.getInt());
            return new StatusKey(cmdID, reportTime);
        }
        catch (Exception e)
        {
            // invalid bigint key
            // return key object that will never match
            return new StatusKey(BigInteger.ZERO, Instant.MAX);
        }
    }
    
    
    Stream<Entry<StatusKey, ICommandStatus>> getStatusByCommand(BigInteger cmdKey)
    {
        StatusKey fromKey = new StatusKey(cmdKey, Instant.MIN);
        StatusKey toKey = new StatusKey(cmdKey, Instant.MAX);
        return map.subMap(fromKey, true, toKey, true).entrySet().stream();
    }
    
    
    Entry<BigInteger, ICommandStatus> toExternalEntry(Entry<StatusKey, ICommandStatus> e)
    {
        return new AbstractMap.SimpleEntry<>(toExternalKey(e.getKey()), e.getValue());
    }


    @Override
    public Stream<Entry<BigInteger, ICommandStatus>> selectEntries(CommandStatusFilter filter, Set<CommandStatusField> fields)
    {
        Stream<Entry<StatusKey, ICommandStatus>> resultStream = null;
        
        // if no command filter used, scan all status reports
        if (filter.getCommandFilter() == null)
        {
            resultStream = map.entrySet().stream();
        }
        
        else
        {
            // stream from list of selected commands
            resultStream = cmdStore.selectEntries(filter.getCommandFilter())
                .flatMap(entry -> getStatusByCommand(entry.getKey()));
        }
            
        // filter with predicate and apply limit
        return resultStream
            .filter(e -> filter.test(e.getValue()))
            .limit(filter.getLimit())
            .map(e -> toExternalEntry(e));
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


    public Set<Entry<BigInteger, ICommandStatus>> entrySet()
    {
        return new AbstractSet<Entry<BigInteger, ICommandStatus>>()
        {
            @Override
            public Iterator<Entry<BigInteger, ICommandStatus>> iterator()
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
    public ICommandStatus get(Object key)
    {
        var k = toInternalKey(key);
        return map.get(k);
    }


    @Override
    public ICommandStatus put(BigInteger key, ICommandStatus cmd)
    {
        StatusKey cmdKey = toInternalKey(key);
        ICommandStatus oldCmd = map.replace(cmdKey, cmd);
        
        if (oldCmd == null)
            throw new IllegalArgumentException("put can only be used to update status entries");
        
        return oldCmd;
    }


    @Override
    public BigInteger add(ICommandStatus status)
    {
        StatusKey key = new StatusKey(
            status.getCommandID(),
            status.getReportTime());
        map.remove(key);
        map.put(key, status);
        return toExternalKey(key);
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
            .map(this::toExternalKey)
            .collect(Collectors.toSet());
    }


    @Override
    public int size()
    {
        return map.size();
    }


    @Override
    public Collection<ICommandStatus> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }


    public boolean remove(Object key, Object val)
    {
        return map.remove(key, val);
    }


    @Override
    public ICommandStatus remove(Object key)
    {
        return map.remove(toInternalKey(key));
    }
    
    
    protected void removeAllStatus(BigInteger cmdID)
    {
        // remove all series and commands
        var first = new StatusKey(cmdID, Instant.MIN);
        var last = new StatusKey(cmdID, Instant.MAX);
        map.subMap(first, last).clear();
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }
    
}
