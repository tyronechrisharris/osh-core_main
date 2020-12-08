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
import org.sensorhub.api.database.IFeatureDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.utils.FileUtils;


/**
 * <p>
 * Implementation of the {@link IFeatureDatabase} interface backed by
 * a single H2 MVStore and a {@link MVFeatureStoreImpl}.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 11, 2019
 */
public class MVFeatureDatabase extends AbstractModule<MVFeatureDatabaseConfig> implements IFeatureDatabase
{
    final static String FEATURE_STORE_NAME = "feature_store";
    
    MVStore mvStore;
    MVFeatureStoreImpl featureStore;
    
    
    @Override
    public void start() throws SensorHubException
    {
        try
        {
            // check file path is valid
            if (!FileUtils.isSafeFilePath(config.storagePath))
                throw new DataStoreException("Storage path contains illegal characters: " + config.storagePath);
            
            MVStore.Builder builder = new MVStore.Builder().fileName(config.storagePath);
            
            if (config.readOnly)
                builder.readOnly();
            
            if (config.memoryCacheSize > 0)
                builder.cacheSize(config.memoryCacheSize/1024);
                                      
            if (config.autoCommitBufferSize > 0)
                builder.autoCommitBufferSize(config.autoCommitBufferSize);
            
            if (config.useCompression)
                builder.compress();
            
            mvStore = builder.open();
            mvStore.setVersionsToKeep(0);
            
            // open procedure store
            featureStore = MVFeatureStoreImpl.open(mvStore, MVDataStoreInfo.builder()
                .withName(FEATURE_STORE_NAME)
                .build());
        }
        catch (Exception e)
        {
            throw new DataStoreException("Error while starting MVStore", e);
        }
    }
    
    
    @Override
    protected void afterStart()
    {
        if (hasParentHub() && config.databaseID > 0)
            getParentHub().getDatabaseRegistry().register(this);
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
    public MVFeatureStoreImpl getFeatureStore()
    {
        checkStarted();
        return featureStore;
    }


    @Override
    public void commit()
    {
        checkStarted();
        mvStore.commit();        
    }
    
    
    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        checkStarted();
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


    @Override
    public boolean isOpen()
    {
        return mvStore != null &&
            !mvStore.isClosed() &&
            isStarted();
    }


    @Override
    public boolean isReadOnly()
    {
        checkStarted();
        return mvStore.isReadOnly();
    }

    
    public MVStore getMVStore()
    {
        checkStarted();
        return mvStore;
    }

}
