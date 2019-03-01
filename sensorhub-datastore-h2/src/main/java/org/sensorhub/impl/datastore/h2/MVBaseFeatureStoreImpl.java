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
import java.time.ZoneOffset;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.datastore.FeatureFilter;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureStore;
import org.sensorhub.api.datastore.SpatialFilter;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import net.opengis.gml.v32.AbstractFeature;


/**
 * <p>
 * Feature Store implementation based on H2 MVStore.<br/>
 * This is just a wrapper for the internal base feature store that uses
 * internal IDs (long) as keys instead of the UID strings.
 * </p>
 * 
 * @param <FeatureKey> Key type
 * @param <V> Value type 
 * @param <F> Filter type
 * 
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVBaseFeatureStoreImpl<V extends AbstractFeature, F extends FeatureFilter> implements IFeatureStore<FeatureKey, V, F>
{
    MVBaseFeatureStoreInternalImpl<V, F> featureStore;
    
    
    protected MVBaseFeatureStoreImpl()
    {
        this.featureStore = new MVBaseFeatureStoreInternalImpl<>();
    }
    
    
    protected MVBaseFeatureStoreImpl<V, F> init(MVStore mvStore, MVFeatureStoreInfo dataStoreInfo)
    {
        return init(mvStore, dataStoreInfo, null);
    }
    
    
    protected MVBaseFeatureStoreImpl<V, F> init(MVStore mvStore, MVFeatureStoreInfo dataStoreInfo, IdProvider idProvider)
    {
        featureStore.init(mvStore, dataStoreInfo, idProvider);
        return this;
    }
        

    @Override
    public String getDatastoreName()
    {
        return featureStore.getDatastoreName();
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        return featureStore.getTimeZone();
    }


    @Override
    public long getNumRecords()
    {
        return featureStore.getNumRecords();
    }


    @Override
    public String getFeatureUriPrefix()
    {
        return featureStore.getFeatureUriPrefix();
    }


    @Override
    public long getNumFeatures()
    {
        return featureStore.getNumFeatures();
    }
    
    
    @Override
    public Stream<String> getAllFeatureIDs()
    {
        return featureStore.getAllFeatureIDs();
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        return featureStore.getFeaturesBbox();
    }


    @Override
    public Stream<Bbox> getFeaturesRegionsBbox(SpatialFilter filter)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Stream<Entry<FeatureKey, V>> selectEntries(F filter)
    {
        return (Stream)featureStore.selectEntries(filter);
    }


    @Override
    public Stream<FeatureKey> selectKeys(F filter)
    {
        return featureStore.selectEntries(filter).map(e -> e.getKey());
    }


    @Override
    public Stream<V> select(F filter)
    {
        return featureStore.selectEntries(filter).map(e -> e.getValue());
    }


    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Stream<FeatureKey> removeEntries(F filter)
    {
        return (Stream)featureStore.removeEntries(filter);
    }


    @Override
    public long countMatchingEntries(F filter, long maxCount)
    {
        return featureStore.countMatchingEntries(filter, maxCount);
    }


    @Override
    public boolean sync()
    {
        return featureStore.sync();
    }


    @Override
    public void backup(OutputStream output) throws IOException
    {
        featureStore.backup(output);
    }


    @Override
    public void restore(InputStream input) throws IOException
    {
        featureStore.restore(input);
    }


    @Override
    public boolean isReadSupported()
    {
        return featureStore.isReadSupported();
    }


    @Override
    public boolean isWriteSupported()
    {
        return featureStore.isWriteSupported();
    }


    @Override
    public void clear()
    {
        featureStore.clear();
    }
    
    
    private MVFeatureKey wrapKey(FeatureKey key, long internalID)
    {
        return new MVFeatureKey.Builder()
                .withFeatureKey((FeatureKey)key)
                .withInternalID(internalID)
                .build();
    }


    @Override
    public boolean containsKey(Object key)
    {
        return get(key) != null;
    }


    @Override
    public boolean containsValue(Object value)
    {
        return featureStore.containsValue(value);
    }


    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Set<Entry<FeatureKey, V>> entrySet()
    {
        return (Set)featureStore.entrySet();
    }


    @Override
    public V get(Object keyObj)
    {
        if (!(keyObj instanceof FeatureKey))
            return null;
        FeatureKey key = (FeatureKey)keyObj;
        
        // get internalID
        Long internalID = featureStore.getInternalID(key);
        if (internalID == null)
            return null;
        
        return featureStore.get(wrapKey(key, internalID));
    }


    @Override
    public boolean isEmpty()
    {
        return featureStore.isEmpty();
    }


    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Set<FeatureKey> keySet()
    {
        Set<MVFeatureKey> mapKeySet = featureStore.keySet();
        
        return new AbstractSet<FeatureKey>()
        {
            @Override
            public Iterator<FeatureKey> iterator()
            {
                return (Iterator)mapKeySet.iterator();
            }

            @Override
            public boolean contains(Object o)
            {
                return containsKey(o);
            }

            @Override
            public int size()
            {
                return featureStore.size();
            }    
        };
    }


    @Override
    public synchronized V put(FeatureKey key, V value)
    {
        // get or create internal ID
        Long internalID = featureStore.ensureInternalID(key);
        return featureStore.put(wrapKey(key, internalID), value);
    }


    @Override
    public void putAll(Map<? extends FeatureKey, ? extends V> map)
    {
        Asserts.checkNotNull(map, Map.class);
        map.forEach((k, v) -> put(k, v));
    }


    @Override
    public synchronized V remove(Object keyObj)
    {
        if (!(keyObj instanceof FeatureKey))
            return null;
        FeatureKey key = (FeatureKey)keyObj;
        
        // get internal ID
        Long internalID = featureStore.getInternalID(key);
        if (internalID == null)
            return null;
        
        // remove from features index
        return featureStore.remove(wrapKey(key, internalID));
    }


    @Override
    public int size()
    {
        return featureStore.size();
    }


    @Override
    public Collection<V> values()
    {
        return featureStore.values();
    }
}
