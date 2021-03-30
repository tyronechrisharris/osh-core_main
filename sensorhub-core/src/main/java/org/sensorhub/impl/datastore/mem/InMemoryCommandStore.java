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
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.sensorhub.api.command.ICommandAck;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStats;
import org.sensorhub.api.datastore.command.CommandStatsQuery;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * In-memory implementation of a command store backed by a {@link java.util.NavigableMap}.
 * This implementation is only used to store the latest procedure state and
 * thus only stores the latest command of each command stream.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 28, 2021
 */
public class InMemoryCommandStore extends InMemoryDataStore implements ICommandStore
{
    static final TemporalFilter ALL_TIMES_FILTER = new TemporalFilter.Builder().withAllTimes().build();
    
    ConcurrentNavigableMap<CmdKey, ICommandAck> map = new ConcurrentSkipListMap<>(new CmdKeyComparator());
    InMemoryCommandStreamStore cmdStreamStore;
    AtomicLong cmdCounter = new AtomicLong();
    
    
    private static class CmdKey
    {    
        long cmdStreamID = 0;
        long receiverID = 0;
        Instant issueTime = null;
        
        CmdKey(long cmdStreamID, long receiverID, Instant issueTime)
        {
            this.cmdStreamID = cmdStreamID;
            this.receiverID = receiverID;
            this.issueTime = issueTime;
        }

        @Override
        public String toString()
        {
            return ObjectUtils.toString(this, true);
        }
    }
    
    
    private static class CmdKeyComparator implements Comparator<CmdKey>
    {
        @Override
        public int compare(CmdKey k1, CmdKey k2)
        {
            // first compare command stream IDs
            int comp = Long.compare(k1.cmdStreamID, k2.cmdStreamID);
            if (comp != 0)
                return comp;
            
            // then compare receiverID IDs
            comp = Long.compare(k1.receiverID, k2.receiverID);
            if (comp != 0)
                return comp;
            
            // don't compare result times
            // always return 0 so we store only the latest result!
            return 0;
        }        
    }
    
    
    public InMemoryCommandStore()
    {
        this.cmdStreamStore = new InMemoryCommandStreamStore(this);
    }
    
    
    BigInteger toPublicKey(CmdKey cmdKey)
    {
        // compute internal ID
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(cmdKey.cmdStreamID);
        //buf.putLong(cmdKey.receiverID);
        buf.putInt((int)(cmdKey.issueTime.getEpochSecond()));
        buf.putInt(cmdKey.issueTime.getNano());
        return new BigInteger(buf.array(), 0, buf.position());
    }
    
    
    CmdKey toInternalKey(Object keyObj)
    {
        Asserts.checkArgument(keyObj instanceof BigInteger);
        BigInteger key = (BigInteger)keyObj;

        try
        {
            // parse from BigInt
            byte[] bigIntBytes = key.toByteArray();
            ByteBuffer buf = ByteBuffer.allocate(16);
            for (int i=0; i<(16-bigIntBytes.length); i++)
                buf.put((byte)0);
            buf.put(bigIntBytes);
            buf.flip();
            
            long dsID = buf.getLong();
            //long receiverID = buf.getLong();
            Instant issueTime = Instant.ofEpochSecond(buf.getInt(), buf.getInt());
            return new CmdKey(dsID, 0, issueTime);
        }
        catch (Exception e)
        {
            // invalid bigint key
            // return key object that will never match
            return new CmdKey(0, 0, Instant.MAX);
        }
    }
    
    
    Stream<Entry<CmdKey, ICommandAck>> getCommandsByCommandStream(long cmdStreamID)
    {
        CmdKey fromKey = new CmdKey(cmdStreamID, 0, null);        
        CmdKey toKey = new CmdKey(cmdStreamID, Long.MAX_VALUE, null);
        
        return map.subMap(fromKey, true, toKey, true).entrySet().stream();
    }
    
    
    Stream<Entry<CmdKey, ICommandAck>> getObsByDataStreamAndFoi(long cmdStreamID, long receiverID)
    {
        CmdKey fromKey = new CmdKey(cmdStreamID, receiverID, null);        
        CmdKey toKey = new CmdKey(cmdStreamID, receiverID, null);
        
        return map.subMap(fromKey, true, toKey, true).entrySet().stream();
    }
    
    
    Entry<BigInteger, ICommandAck> toBigIntEntry(Entry<CmdKey, ICommandAck> e)
    {
        return new AbstractMap.SimpleEntry<>(toPublicKey(e.getKey()), e.getValue());
    }


    @Override
    public Stream<Entry<BigInteger, ICommandAck>> selectEntries(CommandFilter filter, Set<CommandField> fields)
    {
        Stream<Entry<CmdKey, ICommandAck>> resultStream = null;
        
        // if no command stream filter used, scan all commands
        if (filter.getCommandStreamFilter() == null)
        {
            resultStream = map.entrySet().stream();
        }
        
        else
        {
            // stream from list of selected command streams
            resultStream = DataStoreUtils.selectCommandStreamIDs(cmdStreamStore, filter.getCommandStreamFilter())
                .flatMap(id -> getCommandsByCommandStream(id));
        }
            
        // filter with predicate and apply limit
        return resultStream
            .filter(e -> filter.test(e.getValue()))
            .limit(filter.getLimit())
            .map(e -> toBigIntEntry(e));
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


    public Set<Entry<BigInteger, ICommandAck>> entrySet()
    {
        return new AbstractSet<Entry<BigInteger, ICommandAck>>()
        {
            @Override
            public Iterator<Entry<BigInteger, ICommandAck>> iterator()
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
    public ICommandAck get(Object key)
    {
        var k = toInternalKey(key);
        return map.get(k);
    }


    @Override
    public ICommandAck put(BigInteger key, ICommandAck cmd)
    {
        CmdKey cmdKey = toInternalKey(key);
        ICommandAck oldCmd = map.replace(cmdKey, cmd);
        
        if (oldCmd == null)
            throw new IllegalArgumentException("put can only be used to update existing keys");
        
        return oldCmd;
    }


    @Override
    public BigInteger add(ICommandAck cmd)
    {
        CmdKey key = new CmdKey(
            cmd.getCommandStreamID(),
            0,
            cmd.getIssueTime());
        map.remove(key);
        map.put(key, cmd);
        return toPublicKey(key);
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
            .map(this::toPublicKey)
            .collect(Collectors.toSet());
    }


    @Override
    public int size()
    {
        return map.size();
    }


    @Override
    public Collection<ICommandAck> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }


    public boolean remove(Object key, Object val)
    {
        return map.remove(key, val);
    }


    @Override
    public ICommandAck remove(Object key)
    {
        return map.remove(toInternalKey(key));
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }


    @Override
    public ICommandStreamStore getCommandStreams()
    {
        return cmdStreamStore;
    }


    @Override
    public Stream<CommandStats> getStatistics(CommandStatsQuery query)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
}
