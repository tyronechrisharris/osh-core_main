/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.mem;

import java.util.concurrent.Callable;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.DatabaseConfig;
import org.sensorhub.api.database.ISystemStateDatabase;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.module.AbstractModule;


/**
 * <p>
 * In-memory implementation of the system state database.<br/>
 * This is used as default when no other implementation is provided.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemorySystemStateDatabase extends AbstractModule<DatabaseConfig> implements ISystemStateDatabase
{
    int databaseNum = 0;
    ISystemDescStore procStore;
    IFoiStore foiStore;
    IObsStore obsStore;
    ICommandStore cmdStore;
        

    public InMemorySystemStateDatabase()
    {
        this((byte)0);
    }
    
    
    public InMemorySystemStateDatabase(int databaseNum)
    {
        this.databaseNum = databaseNum;
        
        this.procStore = new InMemorySystemStore(databaseNum);
        this.foiStore = new InMemoryFoiStore(databaseNum);
        this.obsStore = new InMemoryObsStore(databaseNum);
        this.cmdStore = new InMemoryCommandStore(databaseNum);
        
        procStore.linkTo(obsStore.getDataStreams());
        foiStore.linkTo(procStore);
        foiStore.linkTo(obsStore);
        obsStore.linkTo(foiStore);
        obsStore.getDataStreams().linkTo(procStore);
        cmdStore.getCommandStreams().linkTo(procStore);
    }
    
    
    @Override
    public ISystemDescStore getSystemDescStore()
    {
        return procStore;
    }


    @Override
    public IFoiStore getFoiStore()
    {
        return foiStore;
    }


    @Override
    public IObsStore getObservationStore()
    {
        return obsStore;
    }


    @Override
    public ICommandStore getCommandStore()
    {
        return cmdStore;
    }


    @Override
    public Integer getDatabaseNum()
    {
        return databaseNum;
    }
    
    
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void commit()
    {
    }


    @Override
    protected void doStart() throws SensorHubException
    {
    }


    @Override
    protected void doStop() throws SensorHubException
    {        
    }


    @Override
    public boolean isOpen()
    {
        return true;
    }


    @Override
    public boolean isReadOnly()
    {
        return false;
    }

}
