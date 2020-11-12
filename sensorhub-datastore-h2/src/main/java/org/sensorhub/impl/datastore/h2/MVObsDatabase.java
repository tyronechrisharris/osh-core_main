/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.util.concurrent.Callable;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.persistence.StorageException;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.utils.FileUtils;


/**
 * <p>
 * Implementation of the {@link IProcedureObsDatabase} interface backed by
 * a single H2 MVStore that contains all maps necessary to store observations,
 * features of interest and procedure history.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 23, 2019
 */
public class MVObsDatabase extends AbstractModule<MVObsDatabaseConfig> implements IProcedureObsDatabase
{
    final static String PROCEDURE_STORE_NAME = "proc_store";
    final static String FOI_STORE_NAME = "foi_store";
    final static String OBS_STORE_NAME = "obs_store";
    
    MVStore mvStore;
    MVProcedureStoreImpl procStore;
    MVObsStoreImpl obsStore;
    MVFoiStoreImpl foiStore;
    
    
    @Override
    public void start() throws SensorHubException
    {
        try
        {
            // check file path is valid
            if (!FileUtils.isSafeFilePath(config.storagePath))
                throw new StorageException("Storage path contains illegal characters: " + config.storagePath);
            
            MVStore.Builder builder = new MVStore.Builder().fileName(config.storagePath);
            
            if (config.memoryCacheSize > 0)
                builder = builder.cacheSize(config.memoryCacheSize/1024);
                                      
            if (config.autoCommitBufferSize > 0)
                builder = builder.autoCommitBufferSize(config.autoCommitBufferSize);
            
            if (config.useCompression)
                builder = builder.compress();
            
            mvStore = builder.open();
            mvStore.setVersionsToKeep(0);
            
            // open procedure store
            procStore = MVProcedureStoreImpl.open(mvStore, MVDataStoreInfo.builder()
                .withName(PROCEDURE_STORE_NAME)
                .build());
            
            // open foi store
            foiStore = MVFoiStoreImpl.open(mvStore, MVDataStoreInfo.builder()
                .withName(FOI_STORE_NAME)
                .build());
            
            // open observation store
            obsStore = MVObsStoreImpl.open(mvStore, MVDataStoreInfo.builder()
                .withName(OBS_STORE_NAME)
                .build());
            
            obsStore.linkTo(foiStore);
            foiStore.linkTo(obsStore);
            obsStore.getDataStreams().linkTo(procStore);
            procStore.linkTo(obsStore.getDataStreams());
        }
        catch (Exception e)
        {
            throw new StorageException("Error while starting MVStore", e);
        }
    }


    @Override
    public void stop() throws SensorHubException
    {
        if (mvStore != null) 
        {
            mvStore.close();
            mvStore = null;
        }
    }


    @Override
    public int getDatabaseID()
    {
        return config.databaseID;
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


    @Override
    public void commit()
    {
        if (mvStore != null)
            mvStore.commit();        
    }
    
    
    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
            
            try
            {
                return transaction.call();
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }

    
    public MVStore getMVStore()
    {
        return mvStore;
    }
}
