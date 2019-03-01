/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.File;
import java.nio.file.Files;
import java.time.ZoneOffset;
import org.h2.mvstore.MVStore;
import org.junit.After;
import org.sensorhub.impl.datastore.AbstractTestFeatureStore;


public class TestMVFeatureStore extends AbstractTestFeatureStore<MVFeatureStoreImpl>
{
    private static String DB_FILE_PREFIX = "test-mvfeatures-";
    protected File dbFile;
    protected MVStore mvStore;
    
    
    protected MVFeatureStoreImpl initStore(String featureUriPrefix, ZoneOffset timeZone) throws Exception
    {
        dbFile = File.createTempFile(DB_FILE_PREFIX, ".dat");
        dbFile.deleteOnExit();
        
        openMVStore();
        
        MVFeatureStoreInfo dataStoreInfo = new MVFeatureStoreInfo.Builder()
                .withName(DATASTORE_NAME)
                .withTimeZone(timeZone)
                .withFeatureUriPrefix(featureUriPrefix)
                .build();
                
        return MVFeatureStoreImpl.create(mvStore, dataStoreInfo);
    }
    
    
    protected void forceReadBackFromStorage()
    {
        openMVStore();
        featureStore = MVFeatureStoreImpl.open(mvStore, DATASTORE_NAME);
    }
    
    
    private void openMVStore()
    {
        if (mvStore != null)
            mvStore.close();
        
        mvStore = new MVStore.Builder()
                .fileName(dbFile.getAbsolutePath())
                .autoCommitBufferSize(10)
                .cacheSize(10)
                .open();
    }
    
    
    @After
    public void cleanup() throws Exception
    {
        if (mvStore != null)
            mvStore.close();
        
        if (dbFile != null) 
        {
            System.out.println("DB file size was " + dbFile.length()/1024 + "KB\n");
            
            Files.list(dbFile.toPath().getParent())
                 .filter(f -> f.getFileName().toString().startsWith(DB_FILE_PREFIX))
                 .forEach(f -> f.toFile().delete());
        }            
    }

}
