/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.feature.FeatureFilter;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.feature.IFeatureFilter;
import org.sensorhub.api.obs.FoiFilter;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.obs.ObsFilter;
import org.sensorhub.api.obs.IFoiStore.FoiField;
import org.sensorhub.impl.datastore.registry.DefaultDatabaseRegistry.LocalFilterInfo;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;
import org.vast.util.Bbox;


/**
 * <p>
 * Implementation of foi store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedFoiStore extends ReadOnlyDataStore<FeatureKey, IGeoFeature, FoiField, IFeatureFilter> implements IFoiStore
{
    DefaultDatabaseRegistry registry;
    FederatedObsDatabase db;
    
    
    FederatedFoiStore(DefaultDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = registry;
        this.db = db;
    }


    @Override
    public String getDatastoreName()
    {
        return getClass().getSimpleName();
    }
    
    
    @Override
    public long getNumFeatures()
    {
        long count = 0;
        for (var db: registry.obsDatabases.values())
            count += db.getFoiStore().getNumFeatures();
        return count;
    }


    @Override
    public long getNumRecords()
    {
        long count = 0;
        for (var db: registry.obsDatabases.values())
            count += db.getFoiStore().getNumRecords();
        return count;
    }


    @Override
    public int size()
    {
        return (int)getNumRecords();
    }


    @Override
    public boolean isEmpty()
    {
        return getNumRecords() == 0;
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        Bbox bbox = new Bbox();
        for (var db: registry.obsDatabases.values())
            bbox.add(db.getFoiStore().getFeaturesBbox());
        return bbox;
    }
    
    
    protected FeatureKey ensureFeatureKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof FeatureKey, "key must be a FeatureKey");
        return (FeatureKey)obj;
    }


    @Override
    public boolean containsKey(Object obj)
    {
        FeatureKey key = ensureFeatureKey(obj);
        
        if (key.getInternalID() > 0)
        {
            // lookup in selected database
            var dbInfo = registry.getLocalDbInfo(key.getInternalID());
            if (dbInfo == null)
                return false;
            else
                return dbInfo.db.getFoiStore().containsKey(new FeatureKey(dbInfo.entryID));
        }
        else
        {
            for (var db: registry.obsDatabases.values())
            {
                if (db.getFoiStore().containsKey(key) == true)
                    return true;
            }
            
            return false;
        }
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: registry.obsDatabases.values())
        {
            if (db.getFoiStore().containsValue(value))
                return true;
        }
        
        return false;
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
    protected Entry<FeatureKey, IGeoFeature> toPublicEntry(int databaseID, Entry<FeatureKey, IGeoFeature> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            e.getValue());
    }


    @Override
    public IGeoFeature get(Object obj)
    {
        FeatureKey key = ensureFeatureKey(obj);
        
        if (key.getInternalID() > 0)
        {
            // lookup in selected database
            var dbInfo = registry.getLocalDbInfo(key.getInternalID());
            if (dbInfo == null)
                return null;
            else
                return dbInfo.db.getFoiStore().get(new FeatureKey(dbInfo.entryID));
        }
        else
        {
            for (var db: registry.obsDatabases.values())
            {
                IGeoFeature f = db.getFoiStore().get(key);
                if (f != null)
                    return f;
            }
            
            return null;
        }
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(FoiFilter filter)
    {
        if (filter.getInternalIDs() != null && filter.getInternalIDs().isSet())
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs().getSet());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = FoiFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getObservationFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = db.obsStore.getFilterDispatchMap(filter.getObservationFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = FoiFilter.Builder
                        .from(filter)
                        .withObservations((ObsFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(FeatureFilter filter)
    {
        if (filter.getInternalIDs() != null && filter.getInternalIDs().isSet())
        {
            var filterDispatchMap = registry.getFilterDispatchMap(filter.getInternalIDs().getSet());            
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = FeatureFilter.Builder.from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(IFeatureFilter filter)
    {
        if (filter instanceof FoiFilter)
            return getFilterDispatchMap((FoiFilter)filter);
        else
            return getFilterDispatchMap((FeatureFilter)filter);
    }


    @Override
    public Stream<Entry<FeatureKey, IGeoFeature>> selectEntries(IFeatureFilter filter, Set<FoiField> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbID = v.databaseID;
                    return v.db.getFoiStore().selectEntries((FeatureFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
        else
        {
            return registry.obsDatabases.values().stream()
                .flatMap(db -> {
                    int dbID = db.getDatabaseID();
                    return db.getFoiStore().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbID, e));
                });
        }
    }


    @Override
    public Set<Entry<FeatureKey, IGeoFeature>> entrySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<Entry<FeatureKey, IGeoFeature>> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getFoiStore().entrySet().stream()
                            .map(e -> toPublicEntry(dbID, e));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedFoiStore.this.size();
            }        
        };
    }


    @Override
    public Set<FeatureKey> keySet()
    {
        return new AbstractSet<>()
        {
            @Override
            public Iterator<FeatureKey> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> {
                        int dbID = db.getDatabaseID();
                        return db.getFoiStore().keySet().stream()
                            .map(k -> toPublicKey(dbID, k));
                    })
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedFoiStore.this.size();
            }        
        };
    }


    @Override
    public Collection<IGeoFeature> values()
    {
        return new AbstractCollection<>()
        {
            @Override
            public Iterator<IGeoFeature> iterator()
            {
                return registry.obsDatabases.values().stream()
                    .flatMap(db -> db.getFoiStore().values().stream())
                    .iterator();
            }

            @Override
            public int size()
            {
                return FederatedFoiStore.this.size();
            }        
        };
    }
    
    
    @Override
    public FeatureKey add(IGeoFeature feature)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public FeatureKey addVersion(IGeoFeature feature)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
    }

}
