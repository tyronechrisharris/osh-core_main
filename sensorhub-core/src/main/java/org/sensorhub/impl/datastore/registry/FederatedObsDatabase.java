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

import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IHistoricalObsDatabase;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.procedure.IProcedureDescriptionStore;


public class FederatedObsDatabase implements IHistoricalObsDatabase
{
    FederatedProcedureStore procStore;
    FederatedFoiStore foiStore;
    FederatedObsStore obsStore;
    

    public FederatedObsDatabase(DefaultDatabaseRegistry registry)
    {
        this.procStore = new FederatedProcedureStore(registry);
        this.foiStore = new FederatedFoiStore(registry);
        this.obsStore = new FederatedObsStore(registry);
        
        // also link stores with each other
        procStore.dataStreamStore = obsStore.dataStreamStore;
        obsStore.dataStreamStore.procStore = procStore;
        obsStore.foiStore = foiStore;
    }
    
    
    @Override
    public IProcedureDescriptionStore getProcedureStore()
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
    public int getDatabaseID()
    {
        throw new UnsupportedOperationException("This method should not be called on the federated database");
    }
}
