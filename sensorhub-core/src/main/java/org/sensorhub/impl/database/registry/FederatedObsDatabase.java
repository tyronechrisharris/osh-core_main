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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;


public class FederatedObsDatabase implements IProcedureObsDatabase
{
    FederatedProcedureStore procStore;
    FederatedFoiStore foiStore;
    FederatedObsStore obsStore;
    IDatabaseRegistry registry;
    
    
    public static class LocalDatabaseInfo
    {
        IProcedureObsDatabase db;
        int databaseNum;
        long entryID;
        BigInteger bigEntryID;
    }
    
    
    public static class LocalFilterInfo
    {
        IProcedureObsDatabase db;
        int databaseNum;
        Set<Long> internalIds = new TreeSet<>();
        Set<BigInteger> bigInternalIds = new TreeSet<>();
        IQueryFilter filter;
    }
    

    public FederatedObsDatabase(IDatabaseRegistry registry)
    {
        this.registry = registry;
        this.procStore = new FederatedProcedureStore(registry, this);
        this.foiStore = new FederatedFoiStore(registry, this);
        this.obsStore = new FederatedObsStore(registry, this);
    }
    
    
    /*
     * Convert from the public ID to the local database ID
     */
    protected LocalDatabaseInfo getLocalDbInfo(long publicID)
    {
        LocalDatabaseInfo dbInfo = new LocalDatabaseInfo();
        dbInfo.databaseNum = registry.getDatabaseNum(publicID);
        dbInfo.db = registry.getObsDatabase(dbInfo.databaseNum);
        dbInfo.entryID = registry.getLocalID(dbInfo.databaseNum, publicID);
        
        if (dbInfo.db == null)
            return null;
        
        return dbInfo;
    }
    
    
    /*
     * Convert from the public ID to the local database ID
     */
    protected LocalDatabaseInfo getLocalDbInfo(BigInteger publicID)
    {
        LocalDatabaseInfo dbInfo = new LocalDatabaseInfo();
        dbInfo.databaseNum = registry.getDatabaseNum(publicID);
        dbInfo.db = registry.getObsDatabase(dbInfo.databaseNum);
        dbInfo.bigEntryID = registry.getLocalID(dbInfo.databaseNum, publicID);
        
        if (dbInfo.db == null)
            return null;
        
        return dbInfo;
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
            if (dbInfo == null)
                continue;
                
            LocalFilterInfo filterInfo = map.computeIfAbsent(dbInfo.databaseNum, k -> new LocalFilterInfo());
            filterInfo.db = dbInfo.db;
            filterInfo.databaseNum = dbInfo.databaseNum;
            filterInfo.internalIds.add(dbInfo.entryID);
        }
        
        return map;
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMapBigInt(Set<BigInteger> publicIDs)
    {
        Map<Integer, LocalFilterInfo> map = new LinkedHashMap<>();
        
        for (BigInteger publicID: publicIDs)
        {
            LocalDatabaseInfo dbInfo = getLocalDbInfo(publicID);
            if (dbInfo == null)
                continue;
                
            LocalFilterInfo filterInfo = map.computeIfAbsent(dbInfo.databaseNum, k -> new LocalFilterInfo());
            filterInfo.db = dbInfo.db;
            filterInfo.databaseNum = dbInfo.databaseNum;
            filterInfo.bigInternalIds.add(dbInfo.bigEntryID);
        }
        
        return map;
    }
    
    
    @Override
    public IProcedureStore getProcedureStore()
    {
        return procStore;
    }


    @Override
    public IObsStore getObservationStore()
    {
        return obsStore;
    }


    @Override
    public IFoiStore getFoiStore()
    {
        return foiStore;
    }
    
    
    protected Collection<IProcedureObsDatabase> getAllDatabases()
    {
        return registry.getRegisteredObsDatabases();
    }


    @Override
    public void commit()
    {
        throw new UnsupportedOperationException(ReadOnlyDataStore.READ_ONLY_ERROR_MSG);
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        throw new UnsupportedOperationException(ReadOnlyDataStore.READ_ONLY_ERROR_MSG);
    }


    @Override
    public Integer getDatabaseNum()
    {
        // should never be called on the federated database
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean isOpen()
    {
        return true;
    }


    @Override
    public boolean isReadOnly()
    {
        return true;
    }
}
