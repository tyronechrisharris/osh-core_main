/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import java.util.concurrent.Callable;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.procedure.IProcedureDescStore;
import org.sensorhub.api.procedure.IProcedureObsDatabase;


public class FederatedObsDatabase implements IProcedureObsDatabase
{
    FederatedProcedureStore procStore;
    FederatedFoiStore foiStore;
    FederatedObsStore obsStore;
    

    public FederatedObsDatabase(DefaultDatabaseRegistry registry)
    {
        this.procStore = new FederatedProcedureStore(registry, this);
        this.foiStore = new FederatedFoiStore(registry, this);
        this.obsStore = new FederatedObsStore(registry, this);
    }
    
    
    @Override
    public IProcedureDescStore getProcedureStore()
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
    public void commit()
    {
        throw new UnsupportedOperationException(ReadOnlyDataStore.READ_ONLY_ERROR_MSG);
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        throw new UnsupportedOperationException(ReadOnlyDataStore.READ_ONLY_ERROR_MSG);
    }


    @Override
    public int getDatabaseID()
    {
        throw new UnsupportedOperationException("This method should not be called on the federated database");
    }
}
