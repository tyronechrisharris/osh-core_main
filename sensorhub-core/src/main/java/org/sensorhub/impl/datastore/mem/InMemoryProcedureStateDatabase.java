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
import org.sensorhub.api.database.IProcedureStateDatabase;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.impl.module.AbstractModule;


/**
 * <p>
 * In-memory implementation of the procedure state database.<br/>
 * This is used as default when no other implementation is provided.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryProcedureStateDatabase extends AbstractModule<DatabaseConfig> implements IProcedureStateDatabase
{
    int databaseNum = 0;
    IProcedureStore procStore;
    IFoiStore foiStore;
    IObsStore obsStore;
        

    public InMemoryProcedureStateDatabase()
    {
        this((byte)0);
    }
    
    
    public InMemoryProcedureStateDatabase(int databaseNum)
    {
        this.databaseNum = databaseNum;
        
        this.procStore = new InMemoryProcedureStore();
        this.foiStore = new InMemoryFoiStore();
        this.obsStore = new InMemoryObsStore();
        
        obsStore.linkTo(foiStore);
        foiStore.linkTo(obsStore);
        obsStore.getDataStreams().linkTo(procStore);
        procStore.linkTo(obsStore.getDataStreams());
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
    public void start() throws SensorHubException
    {
    }


    @Override
    public void stop() throws SensorHubException
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
