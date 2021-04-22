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

import java.math.BigInteger;
import java.util.concurrent.Callable;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
import org.vast.util.Asserts;


public class ProcedureObsDatabaseView implements IProcedureObsDatabase
{
    IProcedureObsDatabase delegate;
    ProcedureStoreView procStoreView;
    FoiStoreView foiStoreView;
    ObsStoreView obsStoreView;
    CommandStoreView commandStoreView;
    
    
    public ProcedureObsDatabaseView(IProcedureObsDatabase delegate, ObsFilter obsFilter)
    {
        this(delegate, obsFilter, null);
    }
    
    
    public ProcedureObsDatabaseView(IProcedureObsDatabase delegate, CommandFilter cmdFilter)
    {
        this(delegate, null, cmdFilter);
    }
    
    
    public ProcedureObsDatabaseView(IProcedureObsDatabase delegate, ObsFilter obsFilter, CommandFilter cmdFilter)
    {
        if (cmdFilter == null)
        {
            // create filter that will never match anything
            cmdFilter = new CommandFilter.Builder()
                .withInternalIDs(BigInteger.ZERO)
                .build();
        }
        
        if (obsFilter == null)
        {
            // create filter that will never match anything
            obsFilter = new ObsFilter.Builder()
                .withInternalIDs(BigInteger.ZERO)
                .build();
        }
        
        init(delegate, obsFilter, cmdFilter);
    }
    
    
    protected void init(IProcedureObsDatabase delegate, ObsFilter obsFilter, CommandFilter cmdFilter)
    {
        this.delegate = Asserts.checkNotNull(delegate, IProcedureObsDatabase.class);
        Asserts.checkNotNull(obsFilter, ObsFilter.class);
        Asserts.checkNotNull(cmdFilter, CommandFilter.class);
        
        var procFilter = obsFilter.getDataStreamFilter() != null ? obsFilter.getDataStreamFilter().getProcedureFilter() : null;
        if (procFilter == null)
            procFilter = cmdFilter.getCommandStreamFilter() != null ? cmdFilter.getCommandStreamFilter().getProcedureFilter() : null;
        
        this.procStoreView = new ProcedureStoreView(delegate.getProcedureStore(), procFilter);
        this.foiStoreView = new FoiStoreView(delegate.getFoiStore(), obsFilter.getFoiFilter());
        this.obsStoreView = new ObsStoreView(delegate.getObservationStore(), obsFilter);
        this.commandStoreView = new CommandStoreView(delegate.getCommandStore(), cmdFilter);
    }
    
    
    @Override
    public Integer getDatabaseNum()
    {
        // need to return the underlying database num so public IDs are
        // computed correctly
        return delegate.getDatabaseNum();
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


    @Override
    public ICommandStore getCommandStore()
    {
        return commandStoreView;
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        throw new UnsupportedOperationException(ReadOnlyDataStore.READ_ONLY_ERROR_MSG);
    }


    @Override
    public void commit()
    {
        throw new UnsupportedOperationException(ReadOnlyDataStore.READ_ONLY_ERROR_MSG);
    }


    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }


    @Override
    public boolean isReadOnly()
    {
        return true;
    }

}
