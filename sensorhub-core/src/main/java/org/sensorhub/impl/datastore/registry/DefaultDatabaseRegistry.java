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

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IHistoricalObsDatabase;
import org.sensorhub.api.datastore.IObsDatabaseRegistry;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.procedure.IProcedureDescriptionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Default implementation of the observation registry allowing only a single
 * database to be registered for all procedures.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 18, 2019
 */
public class DefaultDatabaseRegistry implements IObsDatabaseRegistry
{
    static final Logger log = LoggerFactory.getLogger(DefaultDatabaseRegistry.class);
    
    ISensorHub hub;
    IHistoricalObsDatabase obsDatabase;
    Set<String> registeredProcedures;
    
    static class InternalIdComparator implements Comparator<FeatureId>
    {
        @Override
        public int compare(FeatureId id1, FeatureId id2)
        {
            return Long.compare(id1.getInternalID(), id2.getInternalID());
        }        
    }
    
    
    public DefaultDatabaseRegistry(ISensorHub hub)
    {
        this.hub = hub;
        this.registeredProcedures = new TreeSet<>();
    }


    @Override
    public IProcedureDescriptionStore getProcedureStore()
    {
        checkDatabasePresent();
        return obsDatabase.getProcedureStore();
    }


    @Override
    public IObsStore getObservationStore()
    {
        checkDatabasePresent();
        return obsDatabase.getObservationStore();
    }


    @Override
    public IFoiStore getFoiStore()
    {
        checkDatabasePresent();
        return obsDatabase.getFoiStore();
    }


    @Override
    public synchronized void register(Collection<String> procedureUIDs, IHistoricalObsDatabase db)
    {
        if (obsDatabase == null)
        {
            obsDatabase = db;
            registeredProcedures.addAll(procedureUIDs);
            
            // also register all procedures with procedure registry!
            
        }
        else
            throw new IllegalStateException("Only a single database can be registered with this registry");
    }


    @Override
    public synchronized void unregister(Collection<String> procedureUIDs, IHistoricalObsDatabase db)
    {
        if (obsDatabase == db)
        {
            obsDatabase = null;
            registeredProcedures.clear();
        }
        else
            throw new IllegalStateException("The database instance hasn't been registered here");
    }


    @Override
    public IHistoricalObsDatabase getDatabase(String procedureID)
    {
        return obsDatabase;
    }


    @Override
    public boolean hasDatabase(String procedureUID)
    {
        return registeredProcedures.contains(procedureUID);
    }
    
    
    protected void checkDatabasePresent()
    {
        if (obsDatabase == null)
            throw new IllegalStateException("No observation database has been registered");
    }


    @Override
    public void commit()
    {
        if (obsDatabase != null)
            obsDatabase.commit();
    }


    @Override
    public int getDatabaseID()
    {
        return 0;
    }


    @Override
    public long getPublicID(int databaseID, long dbLocalID)
    {
        return dbLocalID;
    }


    @Override
    public long getLocalID(int databaseID, long publicID)
    {
        return publicID;
    }

}
