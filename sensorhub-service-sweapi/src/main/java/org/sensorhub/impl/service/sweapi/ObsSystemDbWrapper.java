/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import java.util.concurrent.Callable;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.vast.util.Asserts;


public class ObsSystemDbWrapper implements IObsSystemDatabase
{
    IObsSystemDatabase readDb;
    IObsSystemDatabase writeDb;
    IdConverter idConverter;
    IDatabaseRegistry dbRegistry;
    
    
    public ObsSystemDbWrapper(IObsSystemDatabase readDb, IObsSystemDatabase writeDb, IDatabaseRegistry dbRegistry)
    {
        Asserts.checkNotNull(readDb);
        this.readDb = readDb;
        this.writeDb = writeDb;
        this.dbRegistry = Asserts.checkNotNull(dbRegistry, IDatabaseRegistry.class);
        
        // init public <-> internal ID converter
        this.idConverter = new DatabaseRegistryIdConverter(
            Asserts.checkNotNull(dbRegistry, IDatabaseRegistry.class),
            writeDb != null ? writeDb.getDatabaseNum() : 0);
    }
    
    
    @Override
    public Integer getDatabaseNum()
    {
        Asserts.checkState(writeDb != null, "Database is not writable");
        return writeDb.getDatabaseNum();
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        Asserts.checkState(writeDb != null, "Database is not writable");
        return writeDb.executeTransaction(transaction);
    }


    @Override
    public void commit()
    {
        Asserts.checkState(writeDb != null, "Database is not writable");
        writeDb.commit();
    }


    @Override
    public boolean isOpen()
    {
        return writeDb != null ? writeDb.isOpen() : true;
    }


    @Override
    public boolean isReadOnly()
    {
        return writeDb == null;
    }


    @Override
    public ISystemDescStore getSystemDescStore()
    {
        return readDb.getSystemDescStore();
    }


    @Override
    public IFoiStore getFoiStore()
    {
        return readDb.getFoiStore();
    }
    
    
    @Override
    public IDataStreamStore getDataStreamStore()
    {
        return readDb.getDataStreamStore();
    }


    @Override
    public IObsStore getObservationStore()
    {
        return readDb.getObservationStore();
    }


    @Override
    public ICommandStreamStore getCommandStreamStore()
    {
        return readDb.getCommandStreamStore();
    }


    @Override
    public ICommandStore getCommandStore()
    {
        return readDb.getCommandStore();
    }


    public IObsSystemDatabase getReadDb()
    {
        return readDb;
    }


    public IObsSystemDatabase getWriteDb()
    {
        return writeDb;
    }


    public IdConverter getIdConverter()
    {
        return idConverter;
    }
    
    
    public IDatabaseRegistry getDatabaseRegistry()
    {
        return dbRegistry;
    }

}
