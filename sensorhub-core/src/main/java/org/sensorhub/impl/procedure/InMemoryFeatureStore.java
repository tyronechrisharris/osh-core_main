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

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;
import org.sensorhub.api.feature.FeatureFilterBase;
import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.feature.IFeatureStoreBase;
import org.sensorhub.api.feature.IFeatureStoreBase.FeatureField;
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
 * @param <T> Feature type
 * @param <VF> Feature field Type
 * @param <F> Filter type
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public abstract class InMemoryFeatureStore<T extends IFeature, VF extends FeatureField, F extends FeatureFilterBase<? super T>> extends InMemoryDataStore implements IFeatureStoreBase<T, VF, F>
{
    ConcurrentNavigableMap<FeatureKey, T> map = new ConcurrentSkipListMap<>(new InternalIdComparator());
    ConcurrentNavigableMap<String, FeatureKey> uidMap = new ConcurrentSkipListMap<>();
    
    
    /*
     * Use custom comparator so we only keep the latest version of the feature
     * i.e. don't use the time stamp part of the key when indexing
     */
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
    
    
    protected FeatureKey generateKey(T feature)
    {
        String uid = feature.getUniqueIdentifier();
        Asserts.checkNotNull(uid, "uniqueID");        
        if (uidMap.containsKey(uid))
            throw new IllegalArgumentException("Data store already contains a procedure with UID " + uid);
        
        long internalID = map.isEmpty() ? 1 : map.lastKey().getInternalID()+1;
        return new FeatureKey(internalID, FeatureKey.TIMELESS);
    }
    
    
    @Override
    public Entry<FeatureKey, T> getCurrentVersionEntry(String uid)
    {
        FeatureKey key = getCurrentVersionKey(uid);
        if (key == null)
            return null;
        return new AbstractMap.SimpleEntry<>(key, map.get(key));
    }
    
    
    @Override
    public FeatureKey getCurrentVersionKey(String uid)
    {
        Asserts.checkNotNull(uid, "uniqueID");
        return uidMap.get(uid);
    }


    @Override
    public long getNumFeatures()
    {
        return uidMap.size();
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
    public Stream<Entry<FeatureKey, T>> selectEntries(F query, Set<VF> fields)
    {
        Stream<Entry<FeatureKey, T>> resultStream;
        
        if (query.getInternalIDs() != null)
        {
            resultStream = query.getInternalIDs().stream()
                .map(id -> {
                    FeatureKey key = new FeatureKey(id);
                    T val = map.get(key);
                    if (val != null)
                        return (Entry<FeatureKey, T>)new AbstractMap.SimpleEntry<>(key, val);
                    else
                        return null;
                })
                .filter(Objects::nonNull);
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
        FeatureKey fk = ensureFeatureKey(key);
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
        FeatureKey fk = ensureFeatureKey(key);                
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
        Asserts.checkNotNull(val.getUniqueIdentifier(), "uniqueID");
        
        FeatureKey fk = ensureFeatureKey(key);
        T old = map.put(fk, val);
        uidMap.put(val.getUniqueIdentifier(), fk);
        
        return old;
    }


    @Override
    public T remove(Object key)
    {
        FeatureKey fk = ensureFeatureKey(key);
        T proc = map.remove(fk);
        if (proc != null)
            uidMap.remove(proc.getUniqueIdentifier());
        return proc;
    }
    
    
    protected FeatureKey ensureFeatureKey(Object key)
    {
        Asserts.checkArgument(key instanceof FeatureKey, "key must be a FeatureKey");
        return (FeatureKey)key;
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
        FeatureKey fk = ensureFeatureKey(key);
        return map.remove(fk, val);
    }


    @Override
    public FeatureKey add(long parentId, T value)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
