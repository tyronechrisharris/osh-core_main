/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.view;

import java.util.concurrent.Callable;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.procedure.IProcedureStore;
import org.sensorhub.api.procedure.IProcedureObsDatabase;
import org.sensorhub.api.procedure.ProcedureFilter;


public class ProcedureObsFilteredView implements IProcedureObsDatabase
{
    IProcedureObsDatabase delegate;
    ProcedureStoreView procStoreView;
    FoiStoreView foiStoreView;
    ObsStoreView obsStoreView;
    
    
    public ProcedureObsFilteredView(IProcedureObsDatabase delegate, ProcedureFilter filter)
    {
        this.delegate = delegate;
        this.procStoreView = new ProcedureStoreView(delegate.getProcedureStore(), filter);
        this.foiStoreView = new FoiStoreView(delegate.getFoiStore());        
        this.obsStoreView = new ObsStoreView(delegate.getObservationStore());
    }
    
    
    @Override
    public int getDatabaseID()
    {
        // need to return the underlying database ID so public IDs are
        // computed correctly
        return delegate.getDatabaseID();
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void commit()
    {
        throw new UnsupportedOperationException();

    }


    @Override
    public IProcedureStore getProcedureStore()
    {
        return procStoreView;
    }


    @Override
    public IFoiStore getFoiStore()
    {
        return foiStoreView;
    }


    @Override
    public IObsStore getObservationStore()
    {
        return obsStoreView;
    }

}
