/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.database.IDatabase;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IFeatureDatabase;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureEventHandlerDatabase;
import org.sensorhub.utils.MapWithWildcards;
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
    static final Integer DEFAULT_DB_ID = Integer.valueOf(0);
    static final int MAX_NUM_DB = 100;
    static final BigInteger MAX_NUM_DB_BIGINT = BigInteger.valueOf(MAX_NUM_DB);
    static final String END_PREFIX_CHAR = "\n";
    
    ISensorHub hub;
    MapWithWildcards<Integer> obsDatabaseIDs;
    Map<Integer, IProcedureObsDatabase> obsDatabases;
    FederatedObsDatabase globalObsDatabase;
    
    
    
    public DefaultDatabaseRegistry(ISensorHub hub)
    {
        this.hub = hub;
        this.obsDatabaseIDs = new MapWithWildcards<>();
        this.obsDatabases = new ConcurrentSkipListMap<>();
        this.globalObsDatabase = new FederatedObsDatabase(this);
    }
    
    
    @Override
    public synchronized void register(IDatabase db)
    {
        Asserts.checkNotNull(db, IDatabase.class);
        Asserts.checkArgument(db.getDatabaseNum() != null && db.getDatabaseNum() < MAX_NUM_DB, "Database number must be > 0 and < " + MAX_NUM_DB);
        
        log.info("Registering database {} with ID {}", db.getClass().getSimpleName(), db.getDatabaseNum());
        
        if (db instanceof IProcedureObsDatabase)
            registerObsDatabase((IProcedureObsDatabase)db);
        else if (db instanceof IFeatureDatabase)
            registerFeatureDatabase((IFeatureDatabase)db);
    }


    @Override
    public synchronized void register(String procedureUID, IProcedureObsDatabase db)
    {
        Asserts.checkNotNull(db, IProcedureObsDatabase.class);
        
        int databaseID = registerObsDatabase(db);
        registerMapping(procedureUID, databaseID);
    }


    @Override
    public synchronized void register(Collection<String> procedureUIDs, IProcedureObsDatabase db)
    {
        int databaseID = registerObsDatabase(db);
        for (String uid: procedureUIDs)
            registerMapping(uid, databaseID);
    }
    
    
    protected int registerObsDatabase(IProcedureObsDatabase db)
    {
        int dbNum = db.getDatabaseNum();
        
        // add to Id->DB instance map only if not already present        
        if (obsDatabases.putIfAbsent(dbNum, db) != null)
            throw new IllegalStateException("A database with number " + dbNum + " was already registered");
        
        // case of database w/ event handler
        if (db instanceof IProcedureEventHandlerDatabase)
        {
            try
            {
                if (db.isReadOnly())
                    throw new IllegalStateException("Cannot use a read-only database to collect procedure & obs data");
                            
                for (var procUID: ((IProcedureEventHandlerDatabase) db).getHandledProcedures())
                    registerMapping(procUID, dbNum);
            }
            catch (IllegalStateException e)
            {
                // remove database if 2nd registration step failed
                obsDatabases.remove(dbNum);
                throw e;
            }
        }
        
        return dbNum;
    }
    
    
    protected int registerFeatureDatabase(IFeatureDatabase db)
    {
        return 0;
    }
    
    
    protected void registerMapping(String uid, int dbNum)
    {
        Asserts.checkArgument(dbNum > 0, "Database number must be > 0");        
        
        // only insert mapping if not already registered by another database
        // or if registered in state database only (ID 0)
        if (obsDatabaseIDs.putIfAbsent(uid, dbNum) != null)
            throw new IllegalStateException("Procedure " + uid + " is already handled by another database");
        
        // remove all entries from default state DB (DB 0) since it's now handled by another DB
        if (dbNum != 0)
        {
            IProcedureObsDatabase defaultDB = obsDatabases.get(0);
            if (defaultDB != null)
            {
                var procFilter = new ProcedureFilter.Builder()
                    .withUniqueIDs(uid)
                    .build();
                
                var dsFilter = new DataStreamFilter.Builder()
                    .withProcedures(procFilter)
                    .build();
                
                defaultDB.getDataStreamStore().removeEntries(dsFilter);
                var count = defaultDB.getProcedureStore().removeEntries(procFilter);
                
                if (count > 0)
                    log.info("Database #{} now handles procedure(s) {}. Removing all records from state DB", dbNum, uid);
            }
        }
    }
    
    
    @Override
    public synchronized void unregister(IDatabase db)
    {
        Asserts.checkNotNull(db, IDatabase.class);
        
        var it = obsDatabaseIDs.values().iterator();
        while (it.hasNext())
        {
            var dbNum = it.next();
            if (dbNum == db.getDatabaseNum())
                it.remove();
        }
            
        obsDatabases.remove(db.getDatabaseNum());
    }


    @Override
    public synchronized void unregister(String uid, IProcedureObsDatabase db)
    {
        Asserts.checkNotNull(uid, "procedureUID");
        
        if (uid.endsWith("*"))
            uid = uid.substring(0, uid.length()-1) + END_PREFIX_CHAR;
        
        obsDatabaseIDs.remove(uid);
    }
    
    
    @Override
    public IProcedureObsDatabase getObsDatabase(int dbNum)
    {
        Asserts.checkArgument(dbNum >= 0);
        return obsDatabases.get(dbNum);
    }


    @Override
    public IProcedureObsDatabase getObsDatabase(String procUID)
    {
        Integer dbNum = obsDatabaseIDs.get(procUID);
        if (dbNum == null)
            dbNum = DEFAULT_DB_ID;
        return getObsDatabase(dbNum);
    }
    
    
    @Override
    public Collection<IProcedureObsDatabase> getRegisteredObsDatabases()
    {
        return Collections.unmodifiableCollection(obsDatabases.values());
    }


    @Override
    public boolean hasDatabase(String procedureUID)
    {
        return obsDatabaseIDs.containsKey(procedureUID);
    }
    
    
    /*
     * Federated ID management methods
     */    
    
    @Override
    public long getLocalID(int dbNum, long publicID)
    {
        return publicID / MAX_NUM_DB;
    }
    
    
    @Override
    public BigInteger getLocalID(int dbNum, BigInteger publicID)
    {
        return publicID.divide(MAX_NUM_DB_BIGINT);
    }
    
    
    @Override
    public long getPublicID(int dbNum, long entryID)
    {
        return entryID * MAX_NUM_DB + (long)dbNum;
    }
    
    
    @Override
    public BigInteger getPublicID(int dbNum, BigInteger entryID)
    {
        return entryID.multiply(MAX_NUM_DB_BIGINT)
            .add(BigInteger.valueOf(dbNum));
    }
    
    
    @Override
    public int getDatabaseNum(long publicID)
    {
        return (int)(publicID % MAX_NUM_DB);
    }
    
    
    @Override
    public int getDatabaseNum(BigInteger publicID)
    {
        return publicID.mod(MAX_NUM_DB_BIGINT).intValue();
    }


    @Override
    public IProcedureObsDatabase getFederatedObsDatabase()
    {
        return globalObsDatabase;
    }
}
