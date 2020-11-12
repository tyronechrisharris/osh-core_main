/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.mem;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
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
public abstract class InMemoryBaseFeatureStore<T extends IFeature, VF extends FeatureField, F extends FeatureFilterBase<? super T>> extends InMemoryDataStore implements IFeatureStoreBase<T, VF, F>
{
    ConcurrentNavigableMap<FeatureKey, T> map = new ConcurrentSkipListMap<>(new InternalIdComparator());
    ConcurrentNavigableMap<String, FeatureKey> uidMap = new ConcurrentSkipListMap<>();
    ConcurrentNavigableMap<Long, Set<Long>> parentChildMap = new ConcurrentSkipListMap<>();
    Bbox allFeaturesBbox = new Bbox();
    IdProvider<? super T> idProvider = new InMemoryIdProvider<>(1);
    
    
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
        return add(0L, feature);
    }


    @Override
    public FeatureKey add(long parentID, T feature)
    {        
        var uid = DataStoreUtils.checkFeatureObject(feature);
        DataStoreUtils.checkParentFeatureExists(this, parentID);
        
        var existingKey = uidMap.get(uid);
        FeatureKey newKey = generateKey(existingKey, feature);
        if (existingKey != null && existingKey.getValidStartTime().equals(newKey.getValidStartTime()))
            throw new IllegalArgumentException(DataStoreUtils.ERROR_EXISTING_FEATURE_VERSION);     
                
        put(newKey, feature);
        
        // also update parent map
        if (existingKey == null)
        {
            // if needed, add a new feature keyset for the specified parent
            parentChildMap.compute(parentID, (id, keys) -> {
                if (keys == null)
                    keys = new TreeSet<>();
                keys.add(newKey.getInternalID());
                return keys;
            });
        }
        
        return newKey;
    }
    
    
    protected FeatureKey generateKey(FeatureKey existingKey, T f)
    {
        long internalID = existingKey != null ? 
            existingKey.getInternalID() : idProvider.newInternalID(f);
        
        // get valid start time from feature object
        // or use default value (meaning always valid) if no valid time is set
        Instant validStartTime;
        if (f instanceof ITemporalFeature && ((ITemporalFeature)f).getValidTime() != null)
            validStartTime = ((ITemporalFeature)f).getValidTime().begin();
        else
            validStartTime = FeatureKey.TIMELESS;
        
        return new FeatureKey(internalID, validStartTime);
    }
    
    
    @Override
    public boolean contains(long internalID)
    {
        DataStoreUtils.checkInternalID(internalID);
        return map.containsKey(new FeatureKey(internalID));
    }


    @Override
    public boolean contains(String uid)
    {
        DataStoreUtils.checkUniqueID(uid);
        return uidMap.containsKey(uid);
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
        DataStoreUtils.checkUniqueID(uid);
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
        return allFeaturesBbox;
    }


    @Override
    public long getNumRecords()
    {
        return map.size();
    }
    
    
    protected Stream<Entry<FeatureKey, T>> entryStream(Stream<Long> idStream)
    {
        return idStream.map(id -> {
                FeatureKey key = new FeatureKey(id);
                var entry = map.ceilingEntry(key);
                return entry.getKey().getInternalID() == id ? entry : null;
            })
            .filter(Objects::nonNull);
    }
    
    
    protected Stream<Entry<FeatureKey, T>> getFeaturesByParent(long parentID)
    {
        var idStream = parentChildMap.get(parentID).stream();
        return entryStream(idStream);
    }
    
    
    protected Stream<Entry<FeatureKey, T>> getIndexedStream(F filter)
    {
        if (filter.getInternalIDs() != null)
        {
            var idStream = filter.getInternalIDs().stream();
            return entryStream(idStream);
        }
        
        return null;
    }
    
    
    @Override
    public Stream<Entry<FeatureKey, T>> selectEntries(F filter, Set<VF> fields)
    {
        var resultStream = getIndexedStream(filter);        
        
        // if no index used, just scan all features
        if (resultStream == null)
            resultStream = map.entrySet().stream();
        
        return resultStream
            .filter(e -> filter.test(e.getValue()))
            .limit(filter.getLimit());
    }


    @Override
    public void clear()
    {
        map.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        FeatureKey fk = DataStoreUtils.checkFeatureKey(key);
        return map.containsKey(fk);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return map.containsValue(val);
    }


    public Set<Entry<FeatureKey, T>> entrySet()
    {
        return Collections.unmodifiableSet(map.entrySet());
    }
    

    @Override
    public T get(Object key)
    {
        FeatureKey fk = DataStoreUtils.checkFeatureKey(key);                
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
        return Collections.unmodifiableSet(map.keySet());
    }


    @Override
    public T put(FeatureKey key, T feature)
    {
        FeatureKey fk = DataStoreUtils.checkFeatureKey(key);
        DataStoreUtils.checkFeatureObject(feature);
        
        // check that no other feature with same UID exists
        var uid = feature.getUniqueIdentifier();
        var existingKey = uidMap.get(uid);
        if (existingKey != null && existingKey.getInternalID() != key.getInternalID())
            throw new IllegalArgumentException(DataStoreUtils.ERROR_EXISTING_FEATURE + uid);
        
        // skip silently if feature currently in store is newer
        if (existingKey != null && existingKey.getValidStartTime().isAfter(key.getValidStartTime()))
            return map.get(existingKey);
        
        // otherwise update both key and value
        // need to remove 1st to replace key object
        T old = map.remove(fk);
        map.put(fk, feature);
        uidMap.put(feature.getUniqueIdentifier(), fk);
        
        if (feature instanceof IGeoFeature)
            addGeomToBbox((IGeoFeature)feature);
        
        return old;
    }
    
    
    protected void addGeomToBbox(IGeoFeature feature)
    {
        if (feature.getGeometry() != null)
        {
            var env = feature.getGeometry().getGeomEnvelope();
            var ur = env.getUpperCorner();
            var ll = env.getLowerCorner();
            allFeaturesBbox.resizeToContain(ll[0], ll[1], 0);
            allFeaturesBbox.resizeToContain(ur[0], ur[1], 0);
        }
    }


    @Override
    public T remove(Object key)
    {
        FeatureKey fk = DataStoreUtils.checkFeatureKey(key);
        T proc = map.remove(fk);
        if (proc != null)
        {
            uidMap.remove(proc.getUniqueIdentifier());
            
            // also remove parent assoc
            
        }
        return proc;
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
}
