/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureFilter;
import org.sensorhub.api.datastore.IFeatureStore;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.om.IProcedure;
import org.vast.util.Asserts;
import org.vast.util.Bbox;


/**
 * <p>
 * In-memory implementation of a feature store backed by a {@link java.util.NavigableMap}.
 * This implementation is only used to store the latest procedure state and thus
 * doesn't support versioning/history of feature descriptions.
 * </p>
 *
 * @author Alex Robin
 * @param <T> Type of feature object
 * @date Sep 28, 2019
 */
public class InMemoryFeatureStore<T extends IFeature> extends InMemoryDataStore implements IFeatureStore<FeatureKey, T>
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
        FeatureKey newKey = generateKey(feature);
        put(newKey, feature);    
        return newKey;
    }


    @Override
    public FeatureKey addVersion(T feature)
    {
        // create new or replace last version
        FeatureKey fk = uidMap.get(feature.getUniqueIdentifier());
        if (fk == null)
            fk = add(feature);
        else
            put(fk, feature);
        
        return fk;
    }
    
    
    @Override
    public FeatureKey generateKey(T feature)
    {
        String uid = feature.getUniqueIdentifier();
        Asserts.checkNotNull(uid, "UniqueID");
        if (uidMap.containsKey(uid))
            throw new IllegalArgumentException("Data store already contains a procedure with UID " + uid);
        
        long internalID = map.isEmpty() ? 1 : map.lastKey().getInternalID()+1;
        return new FeatureKey(internalID, uid, Instant.MIN);
    }
    
    
    @Override
    public Entry<FeatureKey, T> getLatestVersionEntry(String uid)
    {
        FeatureKey key = getLatestVersionKey(uid);
        if (key == null)
            return null;
        return new AbstractMap.SimpleEntry<>(key, map.get(key));
    }
    
    
    @Override
    public FeatureKey getLatestVersionKey(String uid)
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
    public long getNumRecords()
    {
        return map.size();
    }
    
    
    @Override
    public Stream<Entry<FeatureKey, T>> selectEntries(IFeatureFilter query)
    {
        Stream<Entry<FeatureKey, T>> resultStream;
        
        if (query.getInternalIDs() != null)
        {
            if (query.getInternalIDs().isSet())
            {
                resultStream = query.getInternalIDs().getSet().stream()
                    .map(id -> {
                        FeatureKey key = new FeatureKey(id);
                        T val = map.get(key); 
                        return new AbstractMap.SimpleEntry<>(key, val);
                    });
            }
            else
            {
                var idRange = query.getInternalIDs().getRange();
                resultStream = map.subMap(
                    new FeatureKey(idRange.lowerEndpoint()),
                    new FeatureKey(idRange.upperEndpoint()))
                .entrySet().stream();
            }
            
            
        }
        else
            resultStream = map.entrySet().stream();
        
        return resultStream
            .filter(e -> query.test(e.getValue()))
            .limit(query.getLimit());
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
        
        if (fk.getInternalID() <= 0 && fk.getUniqueID() != null)
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


    public boolean remove(Object key, Object val)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return map.remove(fk, val);
    }
}
