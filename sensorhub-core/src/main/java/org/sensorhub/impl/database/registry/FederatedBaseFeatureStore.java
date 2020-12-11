/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.database.registry.FederatedObsDatabase.LocalFilterInfo;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Bbox;


/**
 * <p>
 * Base federated feature store used as the base for all federated feature
 * stores (features, fois, procedures, etc.)
 * </p>
 * 
 * @param <T> Feature type
 * @param <VF> Feature field Type
 * @param <F> Filter type
 *
 * @author Alex Robin
 * @date Dec 3, 2020
 */
public abstract class FederatedBaseFeatureStore<T extends IFeature, VF extends FeatureField, F extends FeatureFilterBase<? super T>> extends ReadOnlyDataStore<FeatureKey, T, VF, F> implements IFeatureStoreBase<T, VF, F>
{
    final IDatabaseRegistry registry;
    final FederatedObsDatabase parentDb;
    
    
    FederatedBaseFeatureStore(IDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = registry;
        this.parentDb = db;
    }
    
    
    /*
     * Should return the type of store that this class binds to
     */
    protected abstract IFeatureStoreBase<T, VF, F> getFeatureStore(IProcedureObsDatabase db);
    
    
    protected abstract Map<Integer, LocalFilterInfo> getFilterDispatchMap(F filter);
    


    @Override
    public String getDatastoreName()
    {
        return getClass().getSimpleName();
    }
    
    
    @Override
    public long getNumFeatures()
    {
        long count = 0;
        for (var db: parentDb.getAllDatabases())
            count += getFeatureStore(db).getNumFeatures();
        return count;
    }


    @Override
    public long getNumRecords()
    {
        long count = 0;
        for (var db: parentDb.getAllDatabases())
            count += getFeatureStore(db).getNumRecords();
        return count;
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        Bbox bbox = new Bbox();
        for (var db: parentDb.getAllDatabases())
            bbox.add(getFeatureStore(db).getFeaturesBbox());
        return bbox;
    }
    
    
    @Override
    public boolean contains(long internalID)
    {
        DataStoreUtils.checkInternalID(internalID);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(internalID);
        if (dbInfo == null)
            return false;
        else
            return getFeatureStore(dbInfo.db).contains(dbInfo.entryID);
    }


    @Override
    public boolean contains(String uid)
    {
        DataStoreUtils.checkUniqueID(uid);
        
        for (var db: parentDb.getAllDatabases())
        {
            if (getFeatureStore(db).contains(uid))
                return true;
        }
        
        return false;
    }


    @Override
    public boolean containsKey(Object obj)
    {
        FeatureKey key = DataStoreUtils.checkFeatureKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(key.getInternalID());
        if (dbInfo == null)
            return false;
        else
            return getFeatureStore(dbInfo.db).containsKey(new FeatureKey(dbInfo.entryID));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: parentDb.getAllDatabases())
        {
            if (getFeatureStore(db).containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public T get(Object obj)
    {
        FeatureKey key = DataStoreUtils.checkFeatureKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(key.getInternalID());
        if (dbInfo == null)
            return null;
        else
            return getFeatureStore(dbInfo.db).get(
                new FeatureKey(dbInfo.entryID, key.getValidStartTime()));
    }


    @Override
    @SuppressWarnings("unchecked")
    public Stream<Entry<FeatureKey, T>> selectEntries(F filter, Set<VF> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbNum = v.databaseNum;
                    return getFeatureStore(v.db).selectEntries((F)v.filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                })
                .limit(filter.getLimit());
        }
        
        // otherwise scan all DBs
        else
        {
            return parentDb.getAllDatabases().stream()
                .flatMap(db -> {
                    int dbNum = db.getDatabaseNum();
                    return getFeatureStore(db).selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                })
                .limit(filter.getLimit());
        }
    }
    
    
    /*
     * Convert to public IDs on the way out
     */
    protected FeatureId toPublicID(int databaseID, FeatureId id)
    {
        if (id == null)
            return null;
        
        long publicID = registry.getPublicID(databaseID, id.getInternalID());
        return new FeatureId(publicID, id.getUniqueID());
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected FeatureKey toPublicKey(int databaseID, FeatureKey k)
    {
        long publicID = registry.getPublicID(databaseID, k.getInternalID());
        return new FeatureKey(publicID, k.getValidStartTime());
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<FeatureKey, T> toPublicEntry(int databaseID, Entry<FeatureKey, T> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            e.getValue());
    }
    
    
    @Override
    public FeatureKey add(T feature)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public FeatureKey add(long parentId, T value)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }

}
