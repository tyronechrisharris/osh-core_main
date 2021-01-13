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

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.DataStreamAddedEvent;
import org.sensorhub.api.obs.DataStreamDisabledEvent;
import org.sensorhub.api.obs.DataStreamEnabledEvent;
import org.sensorhub.api.obs.DataStreamRemovedEvent;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.procedure.ProcedureDisabledEvent;
import org.sensorhub.api.procedure.ProcedureEnabledEvent;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.api.procedure.ProcedureWrapper;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Asserts;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Helper class for creating/updating/deleting procedures in the associated
 * datastore and publishing the corresponding events.
 * </p>
 *
 * @author Alex Robin
 * @date Dec 21, 2020
 */
public class ProcedureTransactionHandler
{
    final protected ISensorHub hub;
    final protected IProcedureObsDatabase db;
    
    protected long internalID;
    protected String procUID;
    protected String parentGroupUID;
    protected IEventPublisher eventPublisher;
    protected Map<String, FeatureId> foiIdMap = new ConcurrentHashMap<>();
    
    
    public ProcedureTransactionHandler(ISensorHub hub, IProcedureObsDatabase db)
    {
        this.hub = Asserts.checkNotNull(hub, ISensorHub.class);
        this.db = Asserts.checkNotNull(db, IProcedureObsDatabase.class);
    }
    
    
    protected IEventPublisher getEventPublisher()
    {
        if (eventPublisher == null)
        {
            var eventSrcInfo = EventUtils.getProcedureEventSourceInfo(parentGroupUID, procUID);
            eventPublisher = hub.getEventBus().getPublisher(eventSrcInfo);
        }
        
        return eventPublisher;
    }
    
    
    protected IEventPublisher getParentPublisher()
    {
        if (parentGroupUID != null)
        {
            var eventSrcInfo = EventUtils.getProcedureEventSourceInfo(parentGroupUID, procUID);
            return hub.getEventBus().getPublisher(eventSrcInfo);
        }
        
        return hub.getEventPublisher();
    }
    
    
    /**
     * Connect handler to an existing existing procedure using its internal ID.
     * @param id Procedure internal ID
     * @return True if procedure was found, false otherwise
     */
    public boolean connect(long id)
    {
        OshAsserts.checkValidInternalID(id);
        
        // check if it was already initialized
        if (this.internalID != 0)
        {
            if (this.internalID != id)
                throw new IllegalStateException("Internal IDs don't match");
            return true;
        }
        
        // if not, load proc object from DB
        var proc = db.getProcedureStore().getCurrentVersion(id);
        if (proc == null)
            return false;
        
        // cache IDs in this class
        this.internalID = id;
        this.procUID = proc.getUniqueIdentifier();
        return true;
    }
    
    
    /**
     * Connect handler to an existing existing procedure using its unique ID.
     * @param procUID Procedure unique ID
     * @return True if procedure was found, false otherwise
     */
    public boolean connect(String procUID)
    {
        OshAsserts.checkValidUID(procUID);
        
        // check if it was already initialized
        if (this.procUID != null)
        {
            if (!this.procUID.equals(procUID))
                throw new IllegalStateException("Unique IDs don't match");
            return true;
        }
        
        // if not, load proc object from DB
        var procKey = db.getProcedureStore().getCurrentVersionKey(procUID);
        if (procKey == null)
            return false;
        
        // cache IDs in this class
        this.internalID = procKey.getInternalID();
        this.procUID = procUID;
        return true;
    }
    
    
    public boolean create(long parentID, IProcedureWithDesc proc) throws DataStoreException
    {
        if (db.getProcedureStore().contains(proc.getUniqueIdentifier()))
            return false;
        
        var procKey = db.getProcedureStore().add(parentID, proc);
        this.internalID = procKey.getInternalID();
        this.procUID = proc.getUniqueIdentifier();
        
        // send event
        getParentPublisher().publish(new ProcedureAddedEvent(procUID, parentGroupUID));
        
        return true;
    }
    
    
    /**
     * Update the procedure description if it already exists
     * @param proc The new procedure description
     * @return True if the procedure exists and was actually updated, false otherwise
     * @throws DataStoreException if an error occurs during the update
     */
    public boolean update(IProcedureWithDesc proc) throws DataStoreException
    {
        // parent ID is ignored since we don't allow create
        return createOrUpdate(0L, proc, false);
    }
    
    
    public boolean update(AbstractProcess proc) throws DataStoreException
    {
        // parent ID is ignored since we don't allow create
        return createOrUpdate(0L, new ProcedureWrapper(proc), false);
    }
    
    
    /**
     * Update the procedure description or create it if needed.
     * <ul>
     * <li>If no procedure with the given UID already exists, it is created</li>
     * <li>If the validTime is the same as the existing procedure, it is updated.</li>
     * <li>If the validTime is different, a new version of the procedure is created.</li>
     * </ul>
     * @param parentID Internal ID of parent procedure (or 0 if no parent)
     * @param proc The new procedure description
     * @return True if a new procedure was created, false otherwise
     * @throws DataStoreException
     */
    public boolean createOrUpdate(long parentID, IProcedureWithDesc proc) throws DataStoreException
    {
        return createOrUpdate(parentID, proc, true);
    }
    
    
    public boolean createOrUpdate(long parentID, AbstractProcess proc) throws DataStoreException
    {
        return createOrUpdate(parentID, new ProcedureWrapper(proc));
    }
    
    
    protected boolean createOrUpdate(long parentID, IProcedureWithDesc proc, boolean allowCreate) throws DataStoreException
    {
        DataStoreUtils.checkProcedureObject(proc);
        
        if (connect(proc.getUniqueIdentifier()))
        {
            var procKey = db.getProcedureStore().getCurrentVersionKey(proc.getUniqueIdentifier());
            
            var validTime = proc.getFullDescription().getValidTime();
            if (validTime == null || procKey.getValidStartTime().isBefore(validTime.begin()))
            {
                db.getProcedureStore().add(parentID, proc);
                
                // send updated event
                getEventPublisher().publish(new ProcedureChangedEvent(procUID));
            }
            else if (procKey.getValidStartTime().equals(validTime.begin()))
                db.getProcedureStore().put(procKey, proc);
            else
                throw new DataStoreException("A procedure version with a more recent valid time already exists");                
            
            this.internalID = procKey.getInternalID();
            this.procUID = proc.getUniqueIdentifier();
            return false;
        }
        else if (allowCreate)
        {
            create(parentID, proc);
            return true;
        }
        
        return false;
    }
    
    
    public boolean delete()
    {
        checkInitialized();
        
        var procKey = db.getProcedureStore().remove(procUID);
        if (procKey != null)
        {
            // send deleted event
            getParentPublisher().publish(new ProcedureRemovedEvent(procUID, parentGroupUID));
            
            return true;
        }
        
        return false;
    }
    
    
    public void enable()
    {
        checkInitialized();
        getParentPublisher().publish(new ProcedureEnabledEvent(procUID, parentGroupUID));
    }
    
    
    public void disable()
    {
        checkInitialized();
        getParentPublisher().publish(new ProcedureDisabledEvent(procUID, parentGroupUID));
    }
    
    
    
    /*
     * Member helper methods
     */    
    
    public ProcedureTransactionHandler addOrUpdateMember(IProcedureWithDesc proc) throws DataStoreException
    {
        checkInitialized();
        
        var memberHandler = new ProcedureTransactionHandler(hub, db);
        memberHandler.parentGroupUID = procUID;
        memberHandler.createOrUpdate(internalID, proc);
        return memberHandler;
    }
    
    
    public void deleteMember(String uid)
    {
        checkInitialized();
        
        var memberHandler = new ProcedureTransactionHandler(hub, db);
        memberHandler.parentGroupUID = procUID;
        memberHandler.delete();
    }
    
    
    /*
     * Datastream helper methods
     */
    
    public DataStreamTransactionHandler addOrUpdateDataStream(String outputName, DataComponent dataStruct, DataEncoding dataEncoding) throws DataStoreException
    {
        checkInitialized();
        
        var dsHandler = new DataStreamTransactionHandler(hub, db.getObservationStore(), foiIdMap);
        dsHandler.parentGroupUID = parentGroupUID;
        var isNew = dsHandler.createOrUpdate(new ProcedureId(internalID, procUID), outputName, dataStruct, dataEncoding);
        
        if (isNew)
        {
            getEventPublisher().publish(
                new DataStreamAddedEvent(procUID, outputName));
        }
        
        return dsHandler;
    }
    
    
    public void enableDataStream(String outputName)
    {                
        // send event
        getEventPublisher().publish(new DataStreamEnabledEvent(procUID, outputName));
    }
    
    
    public void disableDataStream(String outputName)
    {                
        // send event
        getEventPublisher().publish(new DataStreamDisabledEvent(procUID, outputName));
    }
    
    
    public boolean deleteDataStream(String outputName)
    {
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        checkInitialized();
        
        IDataStreamStore dataStreamStore = db.getDataStreamStore();
        var isDeleted = dataStreamStore.removeAllVersions(procUID, outputName) > 0;
        
        if (isDeleted)
            eventPublisher.publish(new DataStreamRemovedEvent(procUID, outputName));
        
        return isDeleted;
    }
    
    
    /*
     * FOI helper methods
     */    
    
    public boolean addOrUpdateFoi(IGeoFeature foi) throws DataStoreException
    {
        DataStoreUtils.checkFeatureObject(foi);
        checkInitialized();
        
        var uid = foi.getUniqueIdentifier();
        boolean isNew = true;
        
        FeatureKey fk = db.getFoiStore().getCurrentVersionKey(uid);
        
        // store feature description if none was found
        if (fk == null)
            fk = db.getFoiStore().add(foi);
        
        // otherwise add it only if its newer than the one already in storage
        else
        {
            var validTime = foi instanceof ITemporalFeature ? ((ITemporalFeature)foi).getValidTime() : null;
            if (validTime != null && fk.getValidStartTime().isBefore(validTime.begin()))
                fk = db.getFoiStore().add(foi);
            else
                isNew = false;
        }
        
        foiIdMap.put(uid, new FeatureId(fk.getInternalID(), uid));
        
        if (isNew)
        {
            getEventPublisher().publish(new FoiEvent(
                System.currentTimeMillis(),
                procUID,
                foi.getUniqueIdentifier(),
                Instant.now()));
        }
        
        return isNew;
    }
    
    
    protected void checkInitialized()
    {
        Asserts.checkState(internalID > 0 && procUID != null, "procedure handler is not initialized");
    }
    
    
    public long getInternalID()
    {
        return internalID;
    }


    protected String getParentGroupUID()
    {
        return parentGroupUID;
    }


    protected void setParentGroupUID(String parentGroupUID)
    {
        this.parentGroupUID = parentGroupUID;
    }
}
