/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.utils.OshAsserts;
import org.vast.util.Asserts;


/**
 * <p>
 * Helper class for creating/updating/deleting procedures and datastreams in
 * the associated database and publishing the corresponding events.
 * </p>
 *
 * @author Alex Robin
 * @date Dec 21, 2020
 */
public class ProcedureObsTransactionHandler
{
    final protected IEventBus eventBus;
    final protected IProcedureObsDatabase db;
    
    
    public ProcedureObsTransactionHandler(IEventBus eventBus, IProcedureObsDatabase db)
    {
        this.eventBus = Asserts.checkNotNull(eventBus, IEventBus.class);
        this.db = Asserts.checkNotNull(db, IProcedureObsDatabase.class);
    }
    
    
    /**
     * Add a new procedure with the provided description
     * @param proc Procedure description
     * @return The transaction handler linked to the procedure
     * @throws DataStoreException if a procedure with the same UID already exists
     */
    public ProcedureTransactionHandler addProcedure(IProcedureWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        // add procedure to store
        var procKey = db.getProcedureStore().add(proc);
        var procUID = proc.getUniqueIdentifier();
        
        // send event
        var eventPublisher = eventBus.getPublisher(ISensorHub.EVENT_SOURCE_INFO);
        eventPublisher.publish(new ProcedureAddedEvent(procUID, null));
        
        // create new procedure handler
        return createProcedureHandler(procKey, procUID);
    }
    
    
    /**
     * Update the description of an existing procedure
     * @param proc
     * @return The transaction handler linked to the procedure
     * @throws DataStoreException if the procedure doesn't exist or cannot be updated
     */
    public ProcedureTransactionHandler updateProcedure(IProcedureWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        var procHandler = getProcedureHandler(proc.getUniqueIdentifier());
        procHandler.update(proc);
        return procHandler;
    }
    
    
    /**
     * Add or update a procedure.
     * If no procedure with the same UID already exists, a new one will be created,
     * otherwise the existing one will be updated or versioned depending if the the validity
     * period was changed.
     * @param proc New procedure description
     * @return The transaction handler linked to the procedure
     * @throws DataStoreException if the procedure couldn't be added or updated
     */
    public ProcedureTransactionHandler addOrUpdateProcedure(IProcedureWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        var procHandler = getProcedureHandler(proc.getUniqueIdentifier());
        if (procHandler != null)
        {
            procHandler.update(proc);
            return procHandler;
        }
        else
            return addProcedure(proc);
    }
    
    
    /**
     * Create a handler for an existing procedure with the specified ID
     * @param id Procedure internal ID
     * @return The new procedure handler or null if procedure doesn't exist
     */
    public ProcedureTransactionHandler getProcedureHandler(long id)
    {
        OshAsserts.checkValidInternalID(id);
        
        // load proc object from DB
        var procEntry = db.getProcedureStore().getCurrentVersionEntry(id);
        if (procEntry == null)
            return null;
        
        // create new procedure handler
        var procKey = procEntry.getKey();
        var procUID = procEntry.getValue().getUniqueIdentifier();
        return createProcedureHandler(procKey, procUID);
    }
    
    
    /**
     * Create a handler for an existing procedure with the specified unique ID
     * @param procUID Procedure unique ID
     * @return The new procedure handler or null if procedure doesn't exist
     */
    public ProcedureTransactionHandler getProcedureHandler(String procUID)
    {
        OshAsserts.checkValidUID(procUID);
        
        // load proc object from DB
        var procKey = db.getProcedureStore().getCurrentVersionKey(procUID);
        if (procKey == null)
            return null;
        
        // create new procedure handler
        return createProcedureHandler(procKey, procUID);
    }
    
    
    /**
     * Create a handler for an existing datastream with the specified ID
     * @param id Datastream internal ID
     * @return The new datastream handler or null if datastream doesn't exist
     */
    public DataStreamTransactionHandler getDataStreamHandler(long id)
    {
        OshAsserts.checkValidInternalID(id);
        
        // load datastream info from DB
        var dsKey = new DataStreamKey(id);
        var dsInfo = db.getDataStreamStore().get(dsKey);
        if (dsInfo == null)
            return null;
        
        // create new datastream handler
        return new DataStreamTransactionHandler(dsKey, dsInfo, this);
    }
    
    
    /**
     * Create a handler for an existing datastream with the specified procedure and output name
     * @param procUID Procedure unique ID
     * @param outputName Output name
     * @return The new datastream handler or null if datastream doesn't exist
     */
    public DataStreamTransactionHandler getDataStreamHandler(String procUID, String outputName)
    {
        OshAsserts.checkValidUID(procUID);
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        
        // load datastream info from DB
        var dsEntry = db.getDataStreamStore().getLatestVersionEntry(procUID, outputName);
        if (dsEntry == null)
            return null;
        
        // create new datastream handler
        return new DataStreamTransactionHandler(dsEntry.getKey(), dsEntry.getValue(), this);
    }
    
    
    protected ProcedureTransactionHandler createProcedureHandler(FeatureKey procKey, String procUID)
    {
        return new ProcedureTransactionHandler(procKey, procUID, this);
    }
}
