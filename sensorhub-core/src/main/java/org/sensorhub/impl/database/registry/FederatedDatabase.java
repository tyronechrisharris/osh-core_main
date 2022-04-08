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
import org.sensorhub.api.database.IFederatedDatabase;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;


public class FederatedDatabase implements IFederatedDatabase
{
    final FederatedSystemDescStore systemStore;
    final FederatedFoiStore foiStore;
    final FederatedObsStore obsStore;
    final FederatedCommandStore commandStore;
    final FederatedProcedureStore procedureStore;
    final IDatabaseRegistry registry;
    
    
    static class LocalDbInfo<T>
    {
        T db;
        int databaseNum;
        long entryID;
        BigInteger bigEntryID;
    }
    
    static class LocalFilterInfo<T>
    {
        T db;
        int databaseNum;
        Set<Long> internalIds = new TreeSet<>();
        Set<BigInteger> bigInternalIds = new TreeSet<>();
        IQueryFilter filter;
    }
    
    static class ObsSystemDbInfo extends LocalDbInfo<IObsSystemDatabase> { }
    static class ObsSystemDbFilterInfo extends LocalFilterInfo<IObsSystemDatabase> { }
    
    static class ProcedureDbInfo extends LocalDbInfo<IProcedureDatabase> { }
    static class ProcedureDbFilterInfo extends LocalFilterInfo<IProcedureDatabase> { }
    

    public FederatedDatabase(IDatabaseRegistry registry)
    {
        this.registry = registry;
        this.systemStore = new FederatedSystemDescStore(registry, this);
        this.foiStore = new FederatedFoiStore(registry, this);
        this.obsStore = new FederatedObsStore(registry, this);
        this.commandStore = new FederatedCommandStore(registry, this);
        this.procedureStore = new FederatedProcedureStore(registry, this);
    }
    
    
    /*
     * Get local obs DB containing resource with the given public ID
     */
    protected ObsSystemDbInfo getLocalObsDbInfo(long publicID)
    {
        var dbInfo = new ObsSystemDbInfo();
        dbInfo.databaseNum = registry.getDatabaseNum(publicID);
        dbInfo.db = registry.getObsDatabaseByNum(dbInfo.databaseNum);
        dbInfo.entryID = registry.getLocalID(dbInfo.databaseNum, publicID);
        
        if (dbInfo.db == null)
            return null;
        
        return dbInfo;
    }
    
    
    /*
     * Get local obs DB containing resource with the given public ID
     */
    protected ObsSystemDbInfo getLocalObsDbInfo(BigInteger publicID)
    {
        var dbInfo = new ObsSystemDbInfo();
        dbInfo.databaseNum = registry.getDatabaseNum(publicID);
        dbInfo.db = registry.getObsDatabaseByNum(dbInfo.databaseNum);
        dbInfo.bigEntryID = registry.getLocalID(dbInfo.databaseNum, publicID);
        
        if (dbInfo.db == null)
            return null;
        
        return dbInfo;
    }
    
    
    /*
     * Get map with an entry for each DB ID extracted from public IDs
     */
    protected Map<Integer, ObsSystemDbFilterInfo> getObsDbFilterDispatchMap(Set<Long> publicIDs)
    {
        Map<Integer, ObsSystemDbFilterInfo> map = new LinkedHashMap<>();
        
        for (long publicID: publicIDs)
        {
            var dbInfo = getLocalObsDbInfo(publicID);
            if (dbInfo == null)
                continue;
                
            var filterInfo = map.computeIfAbsent(dbInfo.databaseNum, k -> new ObsSystemDbFilterInfo());
            filterInfo.db = dbInfo.db;
            filterInfo.databaseNum = dbInfo.databaseNum;
            filterInfo.internalIds.add(dbInfo.entryID);
        }
        
        return map;
    }
    
    
    protected Map<Integer, ObsSystemDbFilterInfo> getObsDbFilterDispatchMapBigInt(Set<BigInteger> publicIDs)
    {
        Map<Integer, ObsSystemDbFilterInfo> map = new LinkedHashMap<>();
        
        for (BigInteger publicID: publicIDs)
        {
            var dbInfo = getLocalObsDbInfo(publicID);
            if (dbInfo == null)
                continue;
                
            var filterInfo = map.computeIfAbsent(dbInfo.databaseNum, k -> new ObsSystemDbFilterInfo());
            filterInfo.db = dbInfo.db;
            filterInfo.databaseNum = dbInfo.databaseNum;
            filterInfo.bigInternalIds.add(dbInfo.bigEntryID);
        }
        
        return map;
    }
    
    
    /*
     * Get local obs DB containing resource with the given public ID
     */
    protected ProcedureDbInfo getLocalProcDbInfo(long publicID)
    {
        var dbInfo = new ProcedureDbInfo();
        dbInfo.databaseNum = registry.getDatabaseNum(publicID);
        dbInfo.db = registry.getProcedureDatabaseByNum(dbInfo.databaseNum);
        dbInfo.entryID = registry.getLocalID(dbInfo.databaseNum, publicID);
        
        if (dbInfo.db == null)
            return null;
        
        return dbInfo;
    }
    
    
    protected Map<Integer, ProcedureDbFilterInfo> getProcDbFilterDispatchMap(Set<Long> publicIDs)
    {
        Map<Integer, ProcedureDbFilterInfo> map = new LinkedHashMap<>();
        
        for (long publicID: publicIDs)
        {
            var dbInfo = getLocalProcDbInfo(publicID);
            if (dbInfo == null)
                continue;
                
            var filterInfo = map.computeIfAbsent(dbInfo.databaseNum, k -> new ProcedureDbFilterInfo());
            filterInfo.db = dbInfo.db;
            filterInfo.databaseNum = dbInfo.databaseNum;
            filterInfo.internalIds.add(dbInfo.entryID);
        }
        
        return map;
    }
    
    
    @Override
    public ISystemDescStore getSystemDescStore()
    {
        return systemStore;
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


    @Override
    public ICommandStore getCommandStore()
    {
        return commandStore;
    }


    @Override
    public IProcedureStore getProcedureStore()
    {
        return procedureStore;
    }


    @Override
    public IFeatureStore getFeatureStore()
    {
        return null;
    }
    
    
    protected Collection<IObsSystemDatabase> getAllObsDatabases()
    {
        return registry.getObsSystemDatabases();
    }
    
    
    protected Collection<IProcedureDatabase> getAllProcDatabases()
    {
        return registry.getProcedureDatabases();
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
