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
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureFilter;
import org.sensorhub.api.datastore.IFeatureStore;
import org.sensorhub.api.datastore.RangeFilter;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import com.google.common.base.Strings;
import com.google.common.collect.Range;


/**
 * <p>
 * Feature Store implementation based on H2 MVStore.
 * </p>
 * 
 * @param <V> Value type 
 * @param <F> Filter type
 * 
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVBaseFeatureStoreImpl<V extends IFeature> implements IFeatureStore<FeatureKey, V>
{
    private static final String FEATURE_IDS_MAP_NAME = "@feature_ids";
    private static final String FEATURE_RECORDS_MAP_NAME = "@feature_records";
    private static final String SPATIAL_INDEX_MAP_NAME = "@feature_spatial";

    protected MVStore mvStore;
    protected MVDataStoreInfo dataStoreInfo;
    protected MVBTreeMap<String, Long> idsIndex;
    protected MVBTreeMap<FeatureKey, V> featuresIndex;
    protected MVRTreeMap<MVFeatureRef> spatialIndex;
    protected IdProvider idProvider;
    
    
    protected MVBaseFeatureStoreImpl()
    {
    }
    
    
    protected MVBaseFeatureStoreImpl<V> init(MVStore mvStore, MVDataStoreInfo dataStoreInfo, IdProvider idProvider)
    {
        this.mvStore = Asserts.checkNotNull(mvStore, MVStore.class);
        this.dataStoreInfo = Asserts.checkNotNull(dataStoreInfo, MVDataStoreInfo.class);
        
        // feature records map
        String mapName = FEATURE_RECORDS_MAP_NAME + ":" + dataStoreInfo.name;
        this.featuresIndex = mvStore.openMap(mapName, 
                new MVBTreeMap.Builder<FeatureKey, V>()
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
            this.idProvider = () -> {
                if (featuresIndex.isEmpty())
                    return 1;
                else
                    return ((FeatureKey)featuresIndex.lastKey()).getInternalID()+1;
            };
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
    public long getNumFeatures()
    {
        return idsIndex.sizeAsLong();
    }
    
    
    @Override
    public FeatureId getFeatureID(FeatureKey key)
    {
        if (!Strings.isNullOrEmpty(key.getUniqueID()))
        {
            Long internalID = idsIndex.get(key.getUniqueID());
            if (internalID == null)
                return null;
            else
                return new FeatureId(internalID, key.getUniqueID());
        }
        
        if (key.getInternalID() > 0)
        {
            FeatureKey lastKey = getLastVersionKey(key.getInternalID());
            if (lastKey == null)
                return null;
            else
                return new FeatureId(key.getInternalID(), lastKey.getUniqueID());
        }
        
        return null;
    }
    
    
    @Override
    public Stream<FeatureId> getAllFeatureIDs()
    {
        return idsIndex.entrySet().stream()
            .map(e -> new FeatureId(e.getValue(), e.getKey()));
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
    public synchronized FeatureKey add(V feature)
    {
        FeatureKey newKey = generateKey(feature);

        // add to store
        V oldValue = putIfAbsent(newKey, feature);
        Asserts.checkState(oldValue == null, "Duplicate key");
            
        return newKey;
    }
    
    
    @Override
    public synchronized FeatureKey addVersion(V feature)
    {
        String uid = feature.getUniqueIdentifier();
        Asserts.checkNotNull(uid, "uniqueID");
        
        Long internalID = idsIndex.get(uid);
        if (internalID == null)
            return add(feature); // case of first version
        
        // check valid time is older than previous
        Instant validStartTime = getValidStartTime(feature);
        if (!validStartTime.isAfter(getLastVersionKey(internalID).getValidStartTime()))
            throw new IllegalArgumentException("Feature validity must start after the previous version");
        
        // generate key
        var key = new FeatureKey(internalID, uid, validStartTime);
        V oldValue = putIfAbsent(key, feature);
        Asserts.checkState(oldValue == null, "Duplicate key");
        
        return key;
    }
    
    
    @Override
    public FeatureKey generateKey(V feature)
    {
        String uid = feature.getUniqueIdentifier();
        Asserts.checkNotNull(uid, "uniqueID");
        if (idsIndex.containsKey(uid))
            throw new IllegalArgumentException("Feature with UID " + uid + " already exists");
        
        // generate key
        long internalID = idProvider.newInternalID();
        Instant validStartTime = getValidStartTime(feature);
        return new FeatureKey(internalID, uid, validStartTime);
    }
    
    
    protected Instant getValidStartTime(V feature)
    {
        if (feature instanceof ITemporalFeature && ((ITemporalFeature)feature).getValidTime() != null)
            return ((ITemporalFeature)feature).getValidTime().lowerEndpoint();
        
        // return default value for features with no time stamps
        // -> this means they are always valid
        return FeatureKey.TIMELESS;
    }


    @Override
    public FeatureKey getLatestVersionKey(String uid)
    {
        Asserts.checkNotNull(uid, "uniqueID");
        
        Long internalID = idsIndex.get(uid);
        if (internalID != null)
            return getLastVersionKey(internalID);
        return null;
    }


    @Override
    public V getLatestVersion(String uid)
    {
        Asserts.checkNotNull(uid, "uniqueID");
        
        Long internalID = idsIndex.get(uid);
        if (internalID != null)
            return featuresIndex.get(getLastVersionKey(internalID));
        return null;
    }
    
    
    @Override
    public Entry<FeatureKey, V> getLatestVersionEntry(String uid)
    {
        Asserts.checkNotNull(uid, "uniqueID");
        
        Long internalID = idsIndex.get(uid);
        
        if (internalID != null)
            return featuresIndex.getEntry(getLastVersionKey(internalID));
        else
            return null;
    }
    
    
    protected FeatureKey getLastVersionKey(long internalID)
    {
        var afterLast = new FeatureKey(internalID, Instant.MAX);
        var lastKey = featuresIndex.floorKey(afterLast);
        
        if (lastKey != null && lastKey.getInternalID() == internalID)
            return lastKey;
        else
            return null;
    }


    @Override
    public boolean contains(String uid)
    {
        return idsIndex.containsKey(uid);
    }
    
    
    private RangeCursor<FeatureKey, V> getFeatureCursor(long internalID, RangeFilter<Instant> timeFilter)
    {
        FeatureKey first = new FeatureKey(internalID, timeFilter.getMin());
        FeatureKey last = new FeatureKey(internalID, timeFilter.getMax());
        
        // start from first key before selected time range to make sure we include
        // any intersecting feature validity period
        FeatureKey before = featuresIndex.floorKey(first);
        if (before != null && before.getInternalID() == internalID)
            first = before;
        
        return new RangeCursor<>(featuresIndex, first, last);
    }
    
    
    protected Stream<Entry<FeatureKey, V>> getIndexedStream(IFeatureFilter filter)
    {
        Stream<Entry<FeatureKey, V>> resultStream = null;
        Stream<Long> internalIdStream = null;
        
        // get time filter
        final RangeFilter<Instant> timeFilter;
        if (filter.getValidTime() != null)
            timeFilter = filter.getValidTime();
        else
            timeFilter = H2Utils.ALL_TIMES_FILTER;
        boolean lastVersionOnly = timeFilter.getMin() == Instant.MAX && timeFilter.getMax() == Instant.MAX;
        
        // if filtering by internal IDs, use these IDs directly
        if (filter.getInternalIDs() != null)
        {
            if (filter.getInternalIDs().isRange())
            {
                var range = filter.getInternalIDs().getRange();
                var cursor = new RangeCursor<>(
                    featuresIndex,
                    new FeatureKey(range.lowerEndpoint()),
                    new FeatureKey(range.upperEndpoint()));
                resultStream = cursor.entryStream();
            }
            else
                internalIdStream = filter.getInternalIDs().getSet().stream();
        }
        
        // if filtering by UID, use idsIndex as primary
        else if (filter.getFeatureUIDs() != null)
        {
            if (filter.getFeatureUIDs().isRange())
            {
                var range = filter.getFeatureUIDs().getRange();
                var cursor = new RangeCursor<>(
                    idsIndex,
                    range.lowerEndpoint(),
                    range.upperEndpoint());
                internalIdStream = cursor.valueStream(); 
            }
            else
            {
                // concatenate streams for each selected feature UID
                internalIdStream = filter.getFeatureUIDs().getSet().stream()
                        .map(uid -> {
                            Long internalID = idsIndex.get(uid);
                            if (internalID == null)
                                return null; // return null if uid is not found 
                            return internalID;
                        });
            }
        }
        
        // if spatial filter is used, use spatialIndex as primary
        else if (filter.getLocationFilter() != null)
        {
            SpatialKey bbox = H2Utils.getBoundingRectangle(0, filter.getLocationFilter().getRoi());
            final RTreeCursor geoCursor = spatialIndex.findIntersectingKeys(bbox);
            resultStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(geoCursor, Spliterator.DISTINCT), true)
                    .map(k -> {
                        MVFeatureRef ref = spatialIndex.get(k);
                        
                        // filter on time here to avoid unnecessary lookups in featuresIndex
                        Range<Instant> validPeriod = ref.getValidityPeriod();
                        if (validPeriod != null && !lastVersionOnly && !timeFilter.getRange().isConnected(validPeriod))
                            return null;
                        
                        var fk = new FeatureKey(ref.getInternalID(),
                            validPeriod != null ? validPeriod.lowerEndpoint() : Instant.MIN);
                        return featuresIndex.getEntry(fk);
                    })
                    .filter(Objects::nonNull);
        }
        
        // if some procedures were selected by ID
        if (internalIdStream != null)
        {
            resultStream = internalIdStream
                .filter(Objects::nonNull)
                .flatMap(id -> getFeatureCursor(id, timeFilter).entryStream());
        }
        
        return resultStream;
    }


    @Override
    public Stream<Entry<FeatureKey, V>> selectEntries(IFeatureFilter filter)
    {
        var resultStream = getIndexedStream(filter);
        
        // if no suitable index was found, just stream all features
        if (resultStream == null)
            resultStream = featuresIndex.entrySet().stream();
        
        // add exact time predicate
        if (filter.getValidTime() != null)
        {
            var timeFilter = filter.getValidTime();
            if (timeFilter.getMin() == Instant.MAX && timeFilter.getMax() == Instant.MAX)
            {
                // TODO optimize this case!
                resultStream = resultStream.filter(e -> {
                    FeatureKey nextKey = featuresIndex.higherKey(e.getKey());
                    return nextKey == null || nextKey.getInternalID() != e.getKey().getInternalID(); 
                });
            }
            else
                resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        }
        
        // add exact geo predicate
        if (filter.getLocationFilter() != null)
            resultStream = resultStream.filter(e -> filter.testLocation(e.getValue()));
        
        // apply key predicate
        if (filter.getKeyPredicate() != null)
            resultStream = resultStream.filter(e -> filter.testKeyPredicate(e.getKey()));
        
        // apply value predicate
        if (filter.getValuePredicate() != null)
            resultStream = resultStream.filter(e -> filter.testValuePredicate(e.getValue()));
        
        // apply limit
        if (filter.getLimit() < Long.MAX_VALUE)
            resultStream = resultStream.limit(filter.getLimit());
        
        return resultStream;
    }


    @Override
    public long countMatchingEntries(IFeatureFilter filter)
    {
        // TODO implement faster method for some special cases
        // i.e. when no predicates are used
        // can make use of H2 index counting feature
        
        return selectEntries(filter).limit(filter.getLimit()).count();
    }


    @Override
    public void commit()
    {
        featuresIndex.getStore().commit();
        featuresIndex.getStore().sync();
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
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                idsIndex.clear();
                spatialIndex.clear();
                featuresIndex.clear();
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
        FeatureKey fk = ensureKeyWithInternalId(key);
        return fk != null ? featuresIndex.containsKey(fk) : false;
    }


    @Override
    public boolean containsValue(Object value)
    {
        return featuresIndex.containsValue(value);
    }


    @Override
    public Set<Entry<FeatureKey, V>> entrySet()
    {
        return featuresIndex.entrySet();
    }


    @Override
    public V get(Object key)
    {
        FeatureKey fk = ensureKeyWithInternalId(key);
        return fk != null ? featuresIndex.get(fk) : null;
    }
    
    
    protected FeatureKey ensureKeyWithInternalId(Object key)
    {
        Asserts.checkArgument(key instanceof FeatureKey, "key must be a FeatureKey");
        FeatureKey fk = (FeatureKey)key;
        Instant validTime = fk.getValidStartTime();
        Long internalID = fk.getInternalID();
        
        // get internal ID if it was missing
        if (internalID <= 0 && !Strings.isNullOrEmpty(fk.getUniqueID()))
        {
            internalID = idsIndex.get(fk.getUniqueID());
            if (internalID == null)
                return null;
        }
        
        // handle case of last valid time
        if (Instant.MAX.equals(validTime))
            return getLastVersionKey(internalID);
        
        else if (fk.getInternalID() <= 0)
            return new FeatureKey(internalID, validTime);
        
        return fk;
    }


    @Override
    public boolean isEmpty()
    {
        return featuresIndex.isEmpty();
    }


    @Override
    public Set<FeatureKey> keySet()
    {
        return featuresIndex.keySet();
    }
    
    
    protected SpatialKey getSpatialKey(FeatureKey key, IFeature feature)
    {
        if (!(feature instanceof IGeoFeature))
            return null;
        
        IGeoFeature gf = (IGeoFeature)feature;
        if (gf.getGeometry() == null)
            return null;
        
        int hashID = Objects.hash(key.getInternalID(), key.getValidStartTime());
        return H2Utils.getBoundingRectangle(hashID, gf.getGeometry());
    }


    @Override
    public synchronized V put(FeatureKey key, V value)
    {
        Asserts.checkArgument(!Strings.isNullOrEmpty(key.getUniqueID()) && 
            key.getUniqueID() == value.getUniqueIdentifier(), "Unique ID must be set on insert");
        Asserts.checkArgument(key.getValidStartTime() != Instant.MAX, "Incorrect valid time"); // Instant.MAX is reserved for querying last version
        
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                // add to main features index
                V oldValue = featuresIndex.put(key, value);
                
                // add to UID index 
                idsIndex.putIfAbsent(key.getUniqueID(), key.getInternalID());
                
                // if feature has geom, add to spatial index
                SpatialKey spatialKey = getSpatialKey(key, value);
                if (spatialKey != null)
                {
                    Range<Instant> validPeriod = null;
                    if (value instanceof ITemporalFeature)
                        validPeriod = ((ITemporalFeature) value).getValidTime();
                    
                    MVFeatureRef ref = new MVFeatureRef.Builder()
                            .withInternalID(key.getInternalID())
                            .withValidityPeriod(validPeriod)
                            .build();
                    
                    spatialIndex.put(spatialKey, ref);
                }
                
                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public synchronized V remove(Object key)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                FeatureKey fk = ensureKeyWithInternalId(key);
                        
                // remove from main index
                V oldValue = featuresIndex.remove(fk);
                if (oldValue == null)
                    return null;
                
                // remove entry from ID index if no more feature entries are present
                long internalID = fk.getInternalID();
                FeatureKey firstKey = new FeatureKey(internalID);
                FeatureKey nextKey = featuresIndex.ceilingKey(firstKey);
                if (nextKey == null || internalID != nextKey.getInternalID())
                    idsIndex.remove(oldValue.getUniqueIdentifier());
                
                // remove from spatial index
                SpatialKey spatialKey = getSpatialKey(fk, oldValue);
                if (spatialKey != null)
                    spatialIndex.remove(spatialKey);   
                
                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
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
