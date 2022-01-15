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
import org.sensorhub.impl.service.sweapi.feature.FoiStoreWrapper;
import org.sensorhub.impl.service.sweapi.obs.DataStreamStoreWrapper;
import org.sensorhub.impl.service.sweapi.obs.ObsStoreWrapper;
import org.sensorhub.impl.service.sweapi.system.SystemStoreWrapper;
import org.sensorhub.impl.service.sweapi.task.CommandStoreWrapper;
import org.sensorhub.impl.service.sweapi.task.CommandStreamStoreWrapper;
import org.vast.util.Asserts;


public class ObsSystemDbWrapper implements IObsSystemDatabase
{
    IObsSystemDatabase writeDb;
    IdConverter idConverter;
    ISystemDescStore systemStore;
    IDataStreamStore dataStreamStore;
    IObsStore obsStore;
    IFoiStore foiStore;
    ICommandStreamStore commandStreamStore;
    ICommandStore commandStore;
    
    
    public ObsSystemDbWrapper(IObsSystemDatabase readDb, IObsSystemDatabase writeDb, IDatabaseRegistry dbRegistry)
    {
        Asserts.checkNotNull(readDb);
        this.writeDb = writeDb;
        
        // init public <-> internal ID converter
        this.idConverter = new DatabaseRegistryIdConverter(
            Asserts.checkNotNull(dbRegistry, IDatabaseRegistry.class),
            writeDb != null ? writeDb.getDatabaseNum() : 0);
        
        this.systemStore = new SystemStoreWrapper(
            readDb.getSystemDescStore(),
            writeDb != null ? writeDb.getSystemDescStore() : null,
            idConverter);
        
        this.dataStreamStore = new DataStreamStoreWrapper(
            readDb.getDataStreamStore(),
            writeDb != null ? writeDb.getDataStreamStore() : null,
            idConverter);
        
        this.obsStore = new ObsStoreWrapper(
            readDb.getObservationStore(),
            writeDb != null ? writeDb.getObservationStore() : null,
            idConverter);
        
        this.commandStreamStore = new CommandStreamStoreWrapper(
            readDb.getCommandStreamStore(),
            writeDb != null ? writeDb.getCommandStreamStore() : null,
            idConverter);
        
        this.commandStore = new CommandStoreWrapper(
            readDb.getCommandStore(),
            writeDb != null ? writeDb.getCommandStore() : null,
            idConverter);
        
        this.foiStore = new FoiStoreWrapper(
            readDb.getFoiStore(),
            writeDb != null ? writeDb.getFoiStore() : null,
            idConverter);
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
        return systemStore;
    }


    @Override
    public IFoiStore getFoiStore()
    {
        return foiStore;
    }
    
    
    @Override
    public IDataStreamStore getDataStreamStore()
    {
        return dataStreamStore;
    }


    @Override
    public IObsStore getObservationStore()
    {
        return obsStore;
    }


    @Override
    public ICommandStreamStore getCommandStreamStore()
    {
        return commandStreamStore;
    }


    @Override
    public ICommandStore getCommandStore()
    {
        return commandStore;
    }


    public IObsSystemDatabase getWriteDb()
    {
        return writeDb;
    }


    public IdConverter getIdConverter()
    {
        return idConverter;
    }

}
