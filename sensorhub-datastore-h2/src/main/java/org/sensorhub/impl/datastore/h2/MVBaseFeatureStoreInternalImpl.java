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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.Page;
import org.h2.mvstore.RangeCursor;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.SpatialKey;
import org.h2.mvstore.rtree.MVRTreeMap.RTreeCursor;
import org.sensorhub.api.datastore.FeatureFilter;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureStore;
import org.sensorhub.api.datastore.RangeFilter;
import org.sensorhub.api.datastore.SpatialFilter;
import org.vast.ogc.gml.TemporalFeature;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import net.opengis.gml.v32.AbstractFeature;


/**
 * <p>
 * Feature Store implementation based on H2 MVStore.<br/>
 * </p>
 * 
 * @param <V> Value type 
 * @param <F> Filter type
 * 
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVBaseFeatureStoreInternalImpl<V extends AbstractFeature, F extends FeatureFilter> implements IFeatureStore<MVFeatureKey, V, F>
{
    private static final String FEATURE_IDS_MAP_NAME = "@feature_ids";
    private static final String FEATURE_RECORDS_MAP_NAME = "@feature_records";
    private static final String SPATIAL_INDEX_MAP_NAME = "@feature_spatial";

    MVFeatureStoreInfo dataStoreInfo;
    MVBTreeMap<String, Long> idsIndex;
    MVBTreeMap<MVFeatureKey, V> featuresIndex;
    MVRTreeMap<MVFeatureRef> spatialIndex;
    IdProvider idProvider;
    
    
    protected MVBaseFeatureStoreInternalImpl()
    {        
    }
    
    
    protected MVBaseFeatureStoreInternalImpl<V, F> init(MVStore mvStore, MVFeatureStoreInfo dataStoreInfo)
    {
        return init(mvStore, dataStoreInfo, null);
    }
    
    
    protected MVBaseFeatureStoreInternalImpl<V, F> init(MVStore mvStore, MVFeatureStoreInfo dataStoreInfo, IdProvider idProvider)
    {
        Asserts.checkNotNull(mvStore, MVStore.class);
        this.dataStoreInfo = Asserts.checkNotNull(dataStoreInfo, MVFeatureStoreInfo.class);
        
        // feature records map
        String mapName = FEATURE_RECORDS_MAP_NAME + ":" + dataStoreInfo.name;
        this.featuresIndex = mvStore.openMap(mapName, 
                new MVBTreeMap.Builder<MVFeatureKey, V>()
                         .keyType(new MVFeatureKeyDataType())
                         .valueType(new KryoDataType()));
                
        // feature unique IDs to internal IDs map
        mapName = FEATURE_IDS_MAP_NAME + ":" + dataStoreInfo.name;
        this.idsIndex = mvStore.openMap(mapName, 
                new MVBTreeMap.Builder<String, Long>());
                
        // open FOI times map
        mapName = SPATIAL_INDEX_MAP_NAME + ":" + dataStoreInfo.name;
        this.spatialIndex = mvStore.openMap(mapName, 
                new MVRTreeMap.Builder<MVFeatureRef>()
                              .dimensions(3)
                              .valueType(new MVFeatureRefDataType()));
        
        // Id provider
        this.idProvider = idProvider;
        if (idProvider == null) // use default if nothing is set
        {
            long nextInternalID = 1;
            if (!featuresIndex.isEmpty())
                nextInternalID = ((MVFeatureKey)featuresIndex.lastKey()).getInternalID()+1;
            this.idProvider = new DefaultIdProvider(nextInternalID);
        }
        
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
        return featuresIndex.sizeAsLong();
    }


    @Override
    public String getFeatureUriPrefix()
    {
        return dataStoreInfo.getFeatureUriPrefix();
    }


    @Override
    public long getNumFeatures()
    {
        return idsIndex.sizeAsLong();
    }
    
    
    @Override
    public Stream<String> getAllFeatureIDs()
    {
        return idsIndex.keySet().stream();
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        Bbox extent = new Bbox();
        
        Page root = spatialIndex.getRoot();
        for (int i = 0; i < root.getKeyCount(); i++)
        {
            SpatialKey key = (SpatialKey)root.getKey(i);
            extent.add(new Bbox(key.min(0), key.min(1), key.min(2),
                                key.max(0), key.max(1), key.max(2)));
        }
        
        return extent;
    }


    @Override
    public Stream<Bbox> getFeaturesRegionsBbox(SpatialFilter filter)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    
    private RangeCursor<MVFeatureKey, V> getFeatureCursor(long internalID, RangeFilter<Instant> timeFilter)
    {
        MVFeatureKey first = new MVFeatureKey.Builder()
                .withInternalID(internalID)
                .withValidStartTime(timeFilter.getMin())
                .build();
        
        MVFeatureKey last = new MVFeatureKey.Builder()
                .withInternalID(internalID)
                .withValidStartTime(timeFilter.getMax())
                .build();
        
        // start from first key before selected time range to make sure we include
        // any intersecting feature validity period
        MVFeatureKey before = featuresIndex.floorKey(first);
        if (before != null && before.getInternalID() == internalID)
            first = before;
        
        return new RangeCursor<>(featuresIndex, first, last);
    }
    
    
    public Long getInternalID(FeatureKey key)
    {
        return idsIndex.get(key.getUniqueID());
    }
    
    
    public long ensureInternalID(FeatureKey key)
    {
        Long internalID = getInternalID(key);
        if (internalID == null)
        {
            internalID = idProvider.newInternalID();
            idsIndex.put(key.getUniqueID(), internalID);
        }
        
        return internalID;
    }


    @Override
    public Stream<Entry<MVFeatureKey, V>> selectEntries(F filter)
    {
        Stream<Entry<MVFeatureKey, V>> resultStream = null;
           
        // get time filter
        final RangeFilter<Instant> timeFilter;
        if (filter.getValidTime() != null)
            timeFilter = filter.getValidTime();
        else
            timeFilter = H2Utils.ALL_TIMES_FILTER;
        
        // if filtering by ID, use idsIndex as primary
        if (filter.getFeatureIDs() != null)
        {
            Set<String> ids = filter.getFeatureIDs().getIdList();
            
            // concatenate streams for each selected feature UID
            resultStream = ids.stream()
                    .flatMap(uid -> {
                        Long internalID = idsIndex.get(uid);
                        if (internalID == null)
                            return null; // return null/empty stream if uid is not found 
                        return getFeatureCursor(internalID, timeFilter).entryStream();
                    });
        }
        
        // if spatial filter is used, use spatialIndex as primary
        else if (filter.getLocation() != null)
        {
            SpatialKey bbox = H2Utils.getBoundingRectangle(0, filter.getLocation().getRoi());
            final RTreeCursor geoCursor = spatialIndex.findIntersectingKeys(bbox);
            resultStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(geoCursor, Spliterator.DISTINCT), true)
                    .map(k -> {
                        MVFeatureRef ref = spatialIndex.get(k);
                        
                        // filter on time here to avoid unnecessary lookups in featuresIndex
                        Range<Instant> validPeriod = ref.getValidityPeriod();
                        if (validPeriod != null && !timeFilter.getRange().isConnected(validPeriod))
                            return null;
                        
                        MVFeatureKey fk = new MVFeatureKey.Builder()
                                .withInternalID(ref.getInternalID())
                                .withValidStartTime(validPeriod != null ? validPeriod.lowerEndpoint() : Instant.MIN)
                                .build();
                        
                        return featuresIndex.getEntry(fk);
                    })
                    .filter(Objects::nonNull);
        }
        
        // else if filtering only by time
        else if (timeFilter != H2Utils.ALL_TIMES_FILTER)
        {
            resultStream = idsIndex.values().stream()
                .flatMap(id -> getFeatureCursor(id, timeFilter).entryStream());
        }
        
        // else stream all features
        else
            resultStream = featuresIndex.entrySet().stream();
        
        // add exact time predicate
        if (filter.getValidTime() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        // add exact geo predicate
        if (filter.getLocation() != null)
            resultStream = resultStream.filter(e -> filter.testLocation(e.getValue()));
        
        // apply key and value predicates
        if (filter.getKeyPredicate() != null)
            resultStream = resultStream.filter(e -> filter.getKeyPredicate().test(e.getKey()));
        
        if (filter.getValuePredicate() != null)
            resultStream = resultStream.filter(e -> filter.getValuePredicate().test((V)e.getValue()));
        
        if (filter.getLimit() < Long.MAX_VALUE)
            resultStream = resultStream.limit(filter.getLimit());
        
        return resultStream;
    }


    @Override
    public Stream<MVFeatureKey> selectKeys(F filter)
    {
        return selectEntries(filter).map(e -> e.getKey());
    }


    @Override
    public Stream<V> select(F filter)
    {
        return selectEntries(filter).map(e -> e.getValue());
    }


    @Override
    public Stream<MVFeatureKey> removeEntries(F filter)
    {
        // TODO optimize this?
        return selectKeys(filter).peek(k -> remove(k));
    }


    @Override
    public long countMatchingEntries(F filter, long maxCount)
    {
        // TODO implement faster method for some special cases
        // i.e. when no predicates are used
        // can make use of H2 index counting feature
        
        return selectEntries(filter).limit(maxCount).count();
    }


    @Override
    public boolean sync()
    {
        featuresIndex.getStore().sync();
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


    @Override
    public synchronized void clear()
    {
        idsIndex.clear();
        spatialIndex.clear();
        featuresIndex.clear();
    }


    @Override
    public boolean containsKey(Object key)
    {
        if (!(key instanceof MVFeatureKey))
            return false;
        return featuresIndex.containsKey(key);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return featuresIndex.containsValue(value);
    }


    @Override
    public Set<Entry<MVFeatureKey, V>> entrySet()
    {
        return featuresIndex.entrySet();
    }


    @Override
    public V get(Object key)
    {
        if (!(key instanceof MVFeatureKey))
            return null;
        return featuresIndex.get(key);
    }


    @Override
    public boolean isEmpty()
    {
        return featuresIndex.isEmpty();
    }


    @Override
    public Set<MVFeatureKey> keySet()
    {
        return featuresIndex.keySet();
    }
    
    
    protected SpatialKey getSpatialKey(MVFeatureKey key, AbstractFeature feature)
    {
        if (!feature.isSetGeometry())
            return null;
        
        int hashID = Objects.hash(key.getInternalID(), key.getValidStartTime());
        return H2Utils.getBoundingRectangle(hashID, feature.getGeometry());
    }


    @Override
    public synchronized V put(MVFeatureKey key, V value)
    {
        // add to main features index
        V oldValue = featuresIndex.put(key, value);
        
        // add to UID index 
        if (!Strings.isNullOrEmpty(key.getUniqueID()))
            idsIndex.putIfAbsent(key.getUniqueID(), key.getInternalID());
        
        // if feature has geom, add to spatial index
        SpatialKey spatialKey = getSpatialKey(key, value);
        if (spatialKey != null)
        {
            Range<Instant> validPeriod = null;
            if (value instanceof TemporalFeature)
                validPeriod = ((TemporalFeature) value).getValidTime();
            
            MVFeatureRef ref = new MVFeatureRef.Builder()
                    .withInternalID(key.getInternalID())
                    .withValidityPeriod(validPeriod)
                    .build();
            
            spatialIndex.put(spatialKey, ref);
        }
        
        return oldValue;
    }
    
    
    protected void addToSpatialIndex(MVFeatureKey key, Range<Instant> validPeriod)
    {
        
    }
    
    
    protected void addToSpatialIndex(SpatialKey bbox, MVFeatureKey key, Range<Instant> validPeriod)
    {
        
    }


    @Override
    public void putAll(Map<? extends MVFeatureKey, ? extends V> map)
    {
        Asserts.checkNotNull(map, Map.class);
        map.forEach((k, v) -> put(k, v));
    }


    @Override
    public synchronized V remove(Object keyObj)
    {
        if (!(keyObj instanceof MVFeatureKey))
            return null;
        
        // remove from main index
        MVFeatureKey key = (MVFeatureKey)keyObj;
        V oldValue = featuresIndex.remove(key);
        
        // remove entry from ID index if no more feature entries are present
        long internalID = key.getInternalID();
        MVFeatureKey firstKey = new MVFeatureKey.Builder()
                .withInternalID(internalID)
                .build();
        MVFeatureKey nextKey = featuresIndex.ceilingKey(firstKey);
        if (nextKey == null || internalID != nextKey.getInternalID())
            idsIndex.remove(key.getUniqueID());   
        
        // remove from spatial index
        if (oldValue != null)
        {
            SpatialKey spatialKey = getSpatialKey(key, oldValue);
            if (spatialKey != null)
                spatialIndex.remove(spatialKey);
        }   
        
        return oldValue;
    }


    @Override
    public int size()
    {
        return featuresIndex.size();
    }


    @Override
    public Collection<V> values()
    {
        return featuresIndex.values();
    }
}
