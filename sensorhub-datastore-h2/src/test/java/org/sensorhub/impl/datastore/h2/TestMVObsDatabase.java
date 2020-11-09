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
import org.sensorhub.impl.datastore.AbstractTestObsDatabase;


public class TestMVObsDatabase extends AbstractTestObsDatabase<MVObsDatabase>
{
    private static String DB_FILE_PREFIX = "test-mvobsdb-";
    protected File dbFile;
    protected MVStore mvStore;
        
    
    @Override
    protected MVObsDatabase initDatabase() throws Exception
    {
        dbFile = File.createTempFile(DB_FILE_PREFIX, ".dat");
        dbFile.deleteOnExit();        
        return openDatabase();
    }
    
    
    @Override
    protected void forceReadBackFromStorage() throws Exception
    {
        this.obsDb = openDatabase();
    }
    
    
    private MVObsDatabase openDatabase() throws Exception
    {
        MVObsDatabase db = new MVObsDatabase();
        MVObsDatabaseConfig config = new MVObsDatabaseConfig();
        config.storagePath = dbFile.getAbsolutePath();
        db.init(config);
        db.start();
        return db;
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
