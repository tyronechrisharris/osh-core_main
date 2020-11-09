/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.File;
import java.nio.file.Files;
import org.h2.mvstore.MVStore;
import org.junit.After;
import org.sensorhub.impl.datastore.AbstractTestDataStreamStore;


public class TestMVDataStreamStore extends AbstractTestDataStreamStore<MVDataStreamStoreImpl>
{
    private static String DB_FILE_PREFIX = "test-mvobs-";
    protected File dbFile;
    protected MVStore mvStore;
        
    
    protected MVDataStreamStoreImpl initStore() throws Exception
    {
        dbFile = File.createTempFile(DB_FILE_PREFIX, ".dat");
        dbFile.deleteOnExit();        
        return openMVStore();
    }
    
    
    protected void forceReadBackFromStorage()
    {
        this.dataStreamStore = openMVStore();
    }
    
    
    private MVDataStreamStoreImpl openMVStore()
    {
        if (mvStore != null)
            mvStore.close();
        
        mvStore = new MVStore.Builder()
                .fileName(dbFile.getAbsolutePath())
                .autoCommitBufferSize(10)
                .cacheSize(10)
                .open();
        
        var obsStore = MVObsStoreImpl.open(mvStore,
            MVDataStoreInfo.builder()
                .withName("OBS_STORE")
                .build());
        
        return (MVDataStreamStoreImpl)obsStore.getDataStreams();
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
