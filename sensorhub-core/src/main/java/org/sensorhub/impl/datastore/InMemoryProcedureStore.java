/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureFilter;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.IProcedureStore;
import org.vast.ogc.om.IProcedure;
import org.vast.util.Asserts;
import org.vast.util.Bbox;


/**
 * <p>
 * In-memory implementation of procedure store based on {@link java.util.TreeMap}.
 * This implementation doesn't support procedure description history.
 * </p>
 *
 * @author Alex Robin
 * @param <T> type of procedure
 * @date Sep 7, 2019
 */
public class InMemoryProcedureStore<T extends IProcedure> implements IProcedureStore<T>
{
    ConcurrentNavigableMap<FeatureKey, T> map = new ConcurrentSkipListMap<>(new InternalIdComparator());
    ConcurrentNavigableMap<String, FeatureKey> uidMap = new ConcurrentSkipListMap<>();
    
    
    static class InternalIdComparator implements Comparator<FeatureKey>
    {
        @Override
        public int compare(FeatureKey k1, FeatureKey k2)
        {
            return Long.compare(k1.getInternalID(), k2.getInternalID());
        }        
    }
    
    
    @Override
    public synchronized FeatureKey add(T feature)
    {
        String uid = feature.getUniqueIdentifier();
        Asserts.checkNotNull(uid, "UniqueID");
        if (uidMap.containsKey(uid))
            throw new IllegalArgumentException("Data store already contains a procedure with UID " + uid);
        
        long internalID = map.isEmpty() ? 1 : map.lastKey().getInternalID()+1;
        FeatureKey newKey = FeatureKey.builder()
            .withInternalID(internalID)
            .withUniqueID(uid)
            .build();
    
        put(newKey, feature);    
        return newKey;
    }


    @Override
    public FeatureKey addVersion(T feature)
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public Entry<FeatureKey, T> getLastEntry(String uid)
    {
        FeatureKey key = getLastKey(uid);
        if (key == null)
            return null;
        return new AbstractMap.SimpleEntry<>(key, map.get(key));
    }
    
    
    @Override
    public FeatureKey getLastKey(String uid)
    {
        return uidMap.get(uid);
    }


    @Override
    public long getNumFeatures()
    {
        return uidMap.size();
    }


    @Override
    public FeatureId getFeatureID(FeatureKey key)
    {
        if (key.getUniqueID() != null)
        {
            FeatureKey storedKey = uidMap.get(key.getUniqueID());
            return storedKey == null ? null : new FeatureId(storedKey.getInternalID(), storedKey.getUniqueID());
        }
        
        if (key.getInternalID() > 0)
        {
            FeatureKey storedKey = map.ceilingKey(key);
            return storedKey == null || storedKey.getInternalID() != key.getInternalID() ? null : new FeatureId(storedKey.getInternalID(), storedKey.getUniqueID());
        }
        
        return null;
    }


    @Override
    public Stream<FeatureId> getAllFeatureIDs()
    {
        return uidMap.values().stream()
            .map(k -> new FeatureId(k.getInternalID(), k.getUniqueID()));
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        return null;
    }


    @Override
    public String getDatastoreName()
    {
        return null;
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        return null;
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }


    @Override
    public Stream<T> select(IFeatureFilter query)
    {
        return selectEntries(query).map(e -> e.getValue());
    }


    @Override
    public Stream<FeatureKey> selectKeys(IFeatureFilter query)
    {
        return selectEntries(query).map(e -> e.getKey());
    }


    @Override
    public Stream<Entry<FeatureKey, T>> selectEntries(IFeatureFilter query)
    {
        return map.entrySet().stream()
            .filter(e -> query.test(e.getValue()))
            .limit(query.getLimit());
    }


    @Override
    public Stream<FeatureKey> removeEntries(IFeatureFilter query)
    {
        return selectEntries(query)
            .map(e -> e.getKey())
            .peek(k -> remove(k));
    }


    @Override
    public long countMatchingEntries(IFeatureFilter query)
    {
        return selectEntries(query).count();
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
    public void clear()
    {
        map.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.containsKey(fk);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return map.containsValue(val);
    }


    public Set<Entry<FeatureKey, T>> entrySet()
    {
        return map.entrySet();
    }
    

    @Override
    public T get(Object key)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);                
        return map.get(fk);
    }


    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }


    @Override
    public Set<FeatureKey> keySet()
    {
        return map.keySet();
    }


    @Override
    public T put(FeatureKey key, T val)
    {
        Asserts.checkNotNull(key, FeatureKey.class);
        Asserts.checkNotNull(val, IProcedure.class);
        
        FeatureKey fk = ensureKeyWithInternalId(key);
        T old = map.put(fk, val);
        if (key.getUniqueID() != null)
            uidMap.put(key.getUniqueID(), fk);
        
        return old;
    }


    @Override
    public void putAll(Map<? extends FeatureKey, ? extends T> entries)
    {
        for (Entry<? extends FeatureKey, ? extends T> e: entries.entrySet())
            put(e.getKey(), e.getValue());
    }


    @Override
    public T remove(Object key)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        T proc = map.remove(fk);
        if (proc != null)
            uidMap.remove(proc.getUniqueIdentifier());
        return proc;
    }
    
    
    protected FeatureKey ensureKeyWithInternalId(Object key)
    {
        Asserts.checkArgument(key instanceof FeatureKey);
        FeatureKey fk = (FeatureKey)key;
        
        if (fk.getInternalID() == 0 && fk.getUniqueID() != null)
        {
            FeatureKey fullKey = uidMap.get(fk.getUniqueID());
            if (fullKey == null)
                return fk;
            
            return fullKey;
        }
        
        return fk;
    }


    @Override
    public int size()
    {
        return map.size();
    }


    @Override
    public Collection<T> values()
    {
        return Collections.unmodifiableCollection(map.values());
    }


    public T compute(FeatureKey key, BiFunction<? super FeatureKey, ? super T, ? extends T> func)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.compute(fk, func);
    }


    public T computeIfAbsent(FeatureKey key, Function<? super FeatureKey, ? extends T> func)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.computeIfAbsent(fk, func);
    }


    public T computeIfPresent(FeatureKey key, BiFunction<? super FeatureKey, ? super T, ? extends T> func)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.computeIfPresent(fk, func);
    }


    public void forEach(BiConsumer<? super FeatureKey, ? super T> func)
    {
        map.forEach(func);
    }


    public T getOrDefault(Object key, T defaultValue)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.getOrDefault(fk, defaultValue);
    }


    public T merge(FeatureKey key, T val, BiFunction<? super T, ? super T, ? extends T> func)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.merge(fk, val, func);
    }


    public T putIfAbsent(FeatureKey key, T val)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.putIfAbsent(fk, val);
    }


    public boolean remove(Object key, Object val)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.remove(fk, val);
    }


    public boolean replace(FeatureKey key, T arg1, T arg2)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.replace(fk, arg1, arg2);
    }


    public T replace(FeatureKey key, T arg1)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.replace(fk, arg1);
    }


    @Override
    public void linkTo(IObsStore obsStore)
    {        
    }

}
