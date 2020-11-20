/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.obs;

import java.util.Collection;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.DatabaseConfig;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.database.IProcedureObsDbAutoPurgePolicy;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.persistence.StorageException;
import org.sensorhub.api.procedure.IProcedureEventHandlerDatabase;
import org.sensorhub.impl.module.AbstractModule;
import org.vast.util.Asserts;


public class ProcedureObsEventDatabase extends AbstractModule<ProcedureObsEventDatabaseConfig> implements IProcedureEventHandlerDatabase
{
    IProcedureObsDatabase db;
    long lastCommitTime = Long.MIN_VALUE;
    Timer autoPurgeTimer;
    
    
    @Override
    public void start() throws SensorHubException
    {
        if (config.dbConfig == null)
            throw new StorageException("Underlying storage configuration must be provided");
        
        // instantiate and start underlying storage
        DatabaseConfig dbConfig = null;
        try
        {
            dbConfig = (DatabaseConfig)config.dbConfig.clone();
            dbConfig.id = getLocalID();
            dbConfig.name = getName();
            Class<?> clazz = Class.forName(dbConfig.moduleClass);
            
            @SuppressWarnings("unchecked")
            IModule<DatabaseConfig> dbModule = (IModule<DatabaseConfig>)clazz.getDeclaredConstructor().newInstance();
            dbModule.setParentHub(getParentHub());
            dbModule.setConfiguration(dbConfig);
            dbModule.requestInit(true);
            dbModule.requestStart();
            
            this.db = (IProcedureObsDatabase)dbModule;
            Asserts.checkNotNull(db.getProcedureStore(), IProcedureStore.class);
            Asserts.checkNotNull(db.getFoiStore(), IFoiStore.class);
            Asserts.checkNotNull(db.getObservationStore(), IObsStore.class);
            Asserts.checkState(!db.isReadOnly(), "Database is read-only");
        }
        catch (Exception e)
        {
            throw new StorageException("Cannot instantiate underlying database " + dbConfig.moduleClass, e);
        }
        
        // start auto-purge timer thread if policy is specified and enabled
        if (config.autoPurgeConfig != null && config.autoPurgeConfig.enabled)
        {
            final IProcedureObsDbAutoPurgePolicy policy = config.autoPurgeConfig.getPolicy();
            autoPurgeTimer = new Timer();
            TimerTask task = new TimerTask() {
                public void run()
                {
                    policy.trimStorage(db, logger);
                }
            };
            
            autoPurgeTimer.schedule(task, 0, (long)(config.autoPurgeConfig.purgePeriod*1000));
        }
    }
    
    
    @Override
    public synchronized void stop() throws SensorHubException
    {
        if (autoPurgeTimer != null)
        {
            autoPurgeTimer.cancel();
            autoPurgeTimer = null;
        }
        
        if (db != null && db instanceof IModule<?>)
            ((IModule<?>)db).stop();
    }


    @Override
    public Collection<String> getHandledProcedures()
    {
        return Collections.unmodifiableCollection(config.procedureUIDs);
    }
    
    
    @Override
    public boolean isProcessEvents()
    {
        return config.processEvents;
    }
    

    public int getDatabaseID()
    {
        return db.getDatabaseID();
    }


    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        return db.executeTransaction(transaction);
    }


    public IProcedureStore getProcedureStore()
    {
        return db.getProcedureStore();
    }


    public IFoiStore getFoiStore()
    {
        return db.getFoiStore();
    }


    public void commit()
    {
        db.commit();
    }


    public IObsStore getObservationStore()
    {
        return db.getObservationStore();
    }


    public IDataStreamStore getDataStreamStore()
    {
        return db.getDataStreamStore();
    }
    
    
    public IProcedureObsDatabase getWrappedDatabase()
    {
        return db;
    }
}
