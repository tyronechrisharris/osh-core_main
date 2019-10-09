/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.nio.file.Files;
import java.time.ZoneOffset;
import org.h2.mvstore.MVStore;
import org.junit.After;
import org.junit.Test;
import org.sensorhub.impl.datastore.AbstractTestObsStore;


public class TestMVObsStore extends AbstractTestObsStore<MVObsStoreImpl>
{
    private static String DB_FILE_PREFIX = "test-mvobs-";
    protected File dbFile;
    protected MVStore mvStore;
        
    
    protected void initStore(ZoneOffset timeZone) throws Exception
    {
        dbFile = File.createTempFile(DB_FILE_PREFIX, ".dat");
        dbFile.deleteOnExit();
        
        openMVStore();
        
        this.procStore = MVProcedureStoreImpl.create(mvStore, MVDataStoreInfo.builder()
            .withName(PROC_DATASTORE_NAME)
            .build());
        
        this.foiStore = MVFoiStoreImpl.create(mvStore, MVDataStoreInfo.builder()
            .withName(FOI_DATASTORE_NAME)
            .build());
        
        this.obsStore = MVObsStoreImpl.create(mvStore,
            (MVProcedureStoreImpl)procStore,
            (MVFoiStoreImpl)foiStore,
            MVDataStoreInfo.builder()
            .withName(OBS_DATASTORE_NAME)
            .withTimeZone(timeZone)
            .build());
    }
    
    
    protected void forceReadBackFromStorage()
    {
        openMVStore();
        this.procStore = MVProcedureStoreImpl.open(mvStore, PROC_DATASTORE_NAME);
        this.foiStore = MVFoiStoreImpl.open(mvStore, FOI_DATASTORE_NAME);
        this.obsStore = MVObsStoreImpl.open(mvStore, OBS_DATASTORE_NAME, (MVProcedureStoreImpl)procStore, (MVFoiStoreImpl)foiStore);
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
    
    
    @Test
    public void testGetNumRecordsTwoProcedures() throws Exception
    {
        super.testGetNumRecordsTwoProcedures();
        
        // check that 2 series were created
        assertEquals(2, obsStore.obsSeriesMainIndex.size());
    }

}
