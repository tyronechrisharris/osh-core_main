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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IHistoricalObsDatabase;
import org.sensorhub.api.datastore.IDatabaseRegistry;
import org.sensorhub.api.datastore.IQueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;


/**
 * <p>
 * Default implementation of the observation registry allowing to register
 * several observation database instances. In this implementation, a given
 * procedure can only be associated to a single database instance.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 18, 2019
 */
public class DefaultDatabaseRegistry implements IDatabaseRegistry
{
    static final Logger log = LoggerFactory.getLogger(DefaultDatabaseRegistry.class);
    static final int MAX_NUM_DB = 100;
    static final String END_PREFIX_CHAR = "\n";
    
    ISensorHub hub;
    NavigableMap<String, Integer> obsDatabaseIDs;
    Map<Integer, IHistoricalObsDatabase> obsDatabases;
    FederatedObsDatabase globalObsDatabase;
    
    
    static class LocalDatabaseInfo
    {
        IHistoricalObsDatabase db;
        int databaseID;
        long entryID;
    }
    
    
    static class LocalFilterInfo
    {
        IHistoricalObsDatabase db;
        int databaseID;
        Set<Long> internalIds = new TreeSet<>();
        IQueryFilter filter;
    }
    
    
    
    public DefaultDatabaseRegistry(ISensorHub hub)
    {
        this.hub = hub;
        this.obsDatabaseIDs = new TreeMap<>();
        this.obsDatabases = new TreeMap<>();
        this.globalObsDatabase = new FederatedObsDatabase(this);
    }


    @Override
    public synchronized void register(Collection<String> procedureUIDs, IHistoricalObsDatabase db)
    {
        Asserts.checkArgument(db.getDatabaseID() < MAX_NUM_DB, "Database ID must be less than " + MAX_NUM_DB);
        
        // add to Id->DB instance map only if not already present
        obsDatabases.putIfAbsent(db.getDatabaseID(), db);
        
        for (String uid: procedureUIDs)
        {
            if (uid.endsWith("*"))
                uid = uid.substring(0, uid.length()-1) + END_PREFIX_CHAR;
            
            // only insert mapping if not already registered by another database
            // or if registered in state database only (ID 0)
            final var finalUid = uid;
            obsDatabaseIDs.compute(uid, (k, v) -> {
                if (v == null || v == 0 || v == db.getDatabaseID())
                    return db.getDatabaseID();
                throw new IllegalStateException("Procedure " + finalUid + " is already part of another database");
            });
            
            // remove all entries from default state DB (DB 0) since it's now handled by another DB
            if (db.getDatabaseID() != 0)
            {
                IHistoricalObsDatabase defaultDB = obsDatabases.get(0);
                if (defaultDB != null)
                {
                    FeatureKey key = defaultDB.getProcedureStore().remove(uid);
                    if (key != null)
                    {
                        log.info("Database {} now handles procedure {}. Removing all records from state DB", db, uid);
                        defaultDB.getObservationStore().getDataStreams().removeEntries(DataStreamFilter.builder()
                            .withProcedures(key.getInternalID())
                            .build());
                    }
                }
            }
        }
    }
    
    
    /*
     * Convert from the public ID to the local database ID
     */
    protected LocalDatabaseInfo getLocalDbInfo(long publicID)
    {
        LocalDatabaseInfo dbInfo = new LocalDatabaseInfo();
        dbInfo.databaseID = (int)(publicID % MAX_NUM_DB);
        dbInfo.db = obsDatabases.get(dbInfo.databaseID);
        dbInfo.entryID = getLocalID(publicID);
        
        if (dbInfo.db == null)
            return null;
        
        return dbInfo;
    }
    
    
    @Override
    public long getLocalID(int databaseID, long publicID)
    {
        return getLocalID(publicID);
    }
    
    
    protected long getLocalID(long publicID)
    {
        return publicID / MAX_NUM_DB;
    }
    
    
    @Override
    public long getPublicID(int databaseID, long entryID)
    {
        return entryID * MAX_NUM_DB + (long)databaseID;
    }
    
    
    /*
     * Get map with an entry for each DB ID extracted from public IDs
     */
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(Set<Long> publicIDs)
    {
        Map<Integer, LocalFilterInfo> map = new LinkedHashMap<>();
        
        for (long publicID: publicIDs)
        {
            LocalDatabaseInfo dbInfo = getLocalDbInfo(publicID);
            LocalFilterInfo filterInfo = map.computeIfAbsent(dbInfo.databaseID, k -> new LocalFilterInfo());
            filterInfo.db = dbInfo.db;
            filterInfo.databaseID = dbInfo.databaseID;
            filterInfo.internalIds.add(dbInfo.entryID);
        }
        
        return map;
    }


    @Override
    public synchronized void unregister(Collection<String> procedureUIDs, IHistoricalObsDatabase db)
    {
        for (String uid: procedureUIDs)
            obsDatabaseIDs.remove(uid);
    }


    @Override
    public IHistoricalObsDatabase getDatabase(String procUID)
    {
        //Byte dbID = obsDatabaseIDs.get(procedureUID);
        Entry<String, Integer> e = obsDatabaseIDs.floorEntry(procUID);
        if (e == null)
            return null;
        
        Integer dbID = null;
        String key = e.getKey();
        
        // case of wildcard match
        if (key.endsWith(END_PREFIX_CHAR))
        {
            String prefix = key.substring(0, key.length()-1);
            if (procUID.startsWith(prefix))
                dbID = e.getValue(); 
        }
        
        // case of exact match
        else if (key.equals(procUID))
        {
            dbID = e.getValue();
        }
        
        return dbID == null ? null : obsDatabases.get(dbID);
    }


    @Override
    public boolean hasDatabase(String procedureUID)
    {
        return obsDatabaseIDs.containsKey(procedureUID);
    }


    @Override
    public IHistoricalObsDatabase getFederatedObsDatabase()
    {
        return globalObsDatabase;
    }
}
