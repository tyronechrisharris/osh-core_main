/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys;

import java.util.concurrent.Callable;
import org.sensorhub.api.common.IdEncoder;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.service.consys.feature.FoiStoreWrapper;
import org.sensorhub.impl.service.consys.obs.DataStreamStoreWrapper;
import org.sensorhub.impl.service.consys.obs.ObsStoreWrapper;
import org.sensorhub.impl.service.consys.procedure.ProcedureStoreWrapper;
import org.sensorhub.impl.service.consys.system.SystemStoreWrapper;
import org.sensorhub.impl.service.consys.task.CommandStoreWrapper;
import org.sensorhub.impl.service.consys.task.CommandStreamStoreWrapper;
import org.vast.util.Asserts;


public class ObsSystemDbWrapper implements IObsSystemDatabase, IProcedureDatabase
{
    static final String NOT_WRITABLE_MSG = "Database is not writable";
    
    IObsSystemDatabase readDb;
    IObsSystemDatabase writeDb;
    IProcedureStore procedureStore;
    ISystemDescStore systemStore;
    IFoiStore foiStore;
    IDataStreamStore dataStreamStore;
    IObsStore obsStore;
    ICommandStreamStore commandStreamStore;
    ICommandStore commandStore;
    IdEncoders idEncoders;
    
    
    public ObsSystemDbWrapper(IObsSystemDatabase readDb, IObsSystemDatabase writeDb, IdEncoders idEncoders)
    {
        this.readDb = Asserts.checkNotNull(readDb);
        this.writeDb = Asserts.checkNotNull(writeDb);
        
        this.systemStore = new SystemStoreWrapper(
            readDb.getSystemDescStore(),
            writeDb != null ? writeDb.getSystemDescStore() : null);

        this.foiStore = new FoiStoreWrapper(
            readDb.getFoiStore(),
            writeDb != null ? writeDb.getFoiStore() : null);
        
        this.dataStreamStore = new DataStreamStoreWrapper(
            readDb.getDataStreamStore(),
            writeDb != null ? writeDb.getDataStreamStore() : null);
        
        this.obsStore = new ObsStoreWrapper(
            readDb.getObservationStore(),
            writeDb != null ? writeDb.getObservationStore() : null);

        this.commandStreamStore = new CommandStreamStoreWrapper(
            readDb.getCommandStreamStore(),
            writeDb != null ? writeDb.getCommandStreamStore() : null);

        this.commandStore = new CommandStoreWrapper(
            readDb.getCommandStore(),
            writeDb != null ? writeDb.getCommandStore() : null);
        
        if (readDb instanceof IProcedureDatabase)
        {
            this.procedureStore = new ProcedureStoreWrapper(
                ((IProcedureDatabase)readDb).getProcedureStore(),
                writeDb != null && writeDb instanceof IProcedureDatabase ?
                    ((IProcedureDatabase)writeDb).getProcedureStore() : null);
        }
        
        this.idEncoders = Asserts.checkNotNull(idEncoders);
    }


    public IObsSystemDatabase getReadDb()
    {
        return readDb;
    }


    public IObsSystemDatabase getWriteDb()
    {
        return writeDb;
    }


    @Override
    public Integer getDatabaseNum()
    {
        Asserts.checkState(writeDb != null, NOT_WRITABLE_MSG);
        return writeDb.getDatabaseNum();
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        Asserts.checkState(writeDb != null, NOT_WRITABLE_MSG);
        return writeDb.executeTransaction(transaction);
    }


    @Override
    public void commit()
    {
        Asserts.checkState(writeDb != null, NOT_WRITABLE_MSG);
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
        return writeDb == null || writeDb.isReadOnly();
    }


    @Override
    public IProcedureStore getProcedureStore()
    {
        return procedureStore;
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
    
    
    public IdEncoders getIdEncoders()
    {
        return idEncoders;
    }
    
    
    public IdEncoder getProcedureIdEncoder()
    {
        return idEncoders.getProcedureIdEncoder();
    }
    
    
    public IdEncoder getSystemIdEncoder()
    {
        return idEncoders.getSystemIdEncoder();
    }
    
    
    public IdEncoder getFoiIdEncoder()
    {
        return idEncoders.getFoiIdEncoder();
    }
    
    
    public IdEncoder getDataStreamIdEncoder()
    {
        return idEncoders.getDataStreamIdEncoder();
    }
    
    
    public IdEncoder getObsIdEncoder()
    {
        return idEncoders.getObsIdEncoder();
    }
    
    
    public IdEncoder getCommandStreamIdEncoder()
    {
        return idEncoders.getCommandStreamIdEncoder();
    }
    
    
    public IdEncoder getCommandIdEncoder()
    {
        return idEncoders.getCommandIdEncoder();
    }

}
