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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.DataStreamAddedEvent;
import org.sensorhub.api.obs.DataStreamChangedEvent;
import org.sensorhub.api.obs.DataStreamDisabledEvent;
import org.sensorhub.api.obs.DataStreamEnabledEvent;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.DataStreamRemovedEvent;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.procedure.ProcedureDisabledEvent;
import org.sensorhub.api.procedure.ProcedureEnabledEvent;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.utils.DataComponentChecks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


public class ProcedureTransactionHandler
{
    static final Logger log = LoggerFactory.getLogger(ProcedureTransactionHandler.class);
    
    protected final ProcedureObsTransactionHandler rootHandler;
    protected final String procUID;    
    protected FeatureKey procKey;
    protected String parentGroupUID;
    protected Map<String, FeatureId> foiIdMap = new ConcurrentHashMap<>();
    protected boolean newlyCreated;
    
    
    public ProcedureTransactionHandler(FeatureKey procKey, String procUID, ProcedureObsTransactionHandler rootHandler)
    {
        this(procKey, procUID, null, rootHandler);
    }
    
    
    public ProcedureTransactionHandler(FeatureKey procKey, String procUID, String parentGroupUID, ProcedureObsTransactionHandler rootHandler)
    {
        this.procKey = Asserts.checkNotNull(procKey, FeatureKey.class);
        this.procUID = OshAsserts.checkValidUID(procUID);
        this.parentGroupUID = parentGroupUID;
        this.rootHandler = Asserts.checkNotNull(rootHandler);
    }
    
    
    public synchronized boolean update(IProcedureWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        if (!getProcedureStore().contains(proc.getUniqueIdentifier()))
            return false;
        
        var validTime = proc.getFullDescription().getValidTime();
        if (validTime == null || procKey.getValidStartTime().isBefore(validTime.begin()))
        {
            procKey = getProcedureStore().add(proc);
            getEventPublisher().publish(new ProcedureChangedEvent(procUID));
        }
        else if (procKey.getValidStartTime().equals(validTime.begin()))
        {
            getProcedureStore().put(procKey, proc);
        }
        else
            throw new DataStoreException("A version of the procedure description with a more recent valid time already exists");                
        
        return true;
    }
    
    
    public boolean delete() throws DataStoreException
    {
        // check if we have a parent
        checkParent();
        
        // error if associated datastreams still exist
        var procDsFilter = new DataStreamFilter.Builder()
            .withProcedures(procKey.getInternalID())
            .build();
        if (getDataStreamStore().countMatchingEntries(procDsFilter) > 0)
            throw new DataStoreException("Procedure cannot be deleted because it is referenced by a datastream");
        
        // remove from datastore
        var procKey = getProcedureStore().remove(procUID);
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
        checkParent();
        getParentPublisher().publish(new ProcedureEnabledEvent(procUID, parentGroupUID));
    }
    
    
    public void disable()
    {
        checkParent();
        getParentPublisher().publish(new ProcedureDisabledEvent(procUID, parentGroupUID));
    }
    
    
    
    /*
     * Member helper methods
     */    
    
    public synchronized ProcedureTransactionHandler addMember(IProcedureWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        var parentGroupUID = this.procUID;
        var parentGroupID = this.procKey.getInternalID();
        
        // add to datastore
        var memberKey = getProcedureStore().add(parentGroupID, proc);
        var memberUID = proc.getUniqueIdentifier();
        
        // send event        
        getEventPublisher().publish(new ProcedureAddedEvent(memberUID, parentGroupUID));
        
        // create the new procedure handler
        return createMemberProcedureHandler(memberKey, memberUID);
    }
    
    
    public synchronized ProcedureTransactionHandler addOrUpdateMember(IProcedureWithDesc proc) throws DataStoreException
    {
        var uid = OshAsserts.checkProcedureObject(proc);
        
        var memberKey = getProcedureStore().getCurrentVersionKey(uid);        
        if (memberKey != null)
        {
            var memberHandler = createMemberProcedureHandler(memberKey, uid);
            memberHandler.update(proc);
            return memberHandler;
        }
        else
            return addMember(proc);
    }
    
    
    
    /*
     * Datastream helper methods
     */
    
    public synchronized DataStreamTransactionHandler addOrUpdateDataStream(String outputName, DataComponent dataStruct, DataEncoding dataEncoding) throws DataStoreException
    {
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        Asserts.checkNotNull(dataStruct, DataComponent.class);
        Asserts.checkNotNull(dataEncoding, DataEncoding.class);
        
        // check output name == data component name
        if (dataStruct.getName() == null)
            dataStruct.setName(outputName);
        else if (!outputName.equals(dataStruct.getName()))
            throw new IllegalStateException("Inconsistent output name");
        
        // try to retrieve existing data stream
        IDataStreamStore dataStreamStore = rootHandler.db.getDataStreamStore();
        Entry<DataStreamKey, IDataStreamInfo> dsEntry = dataStreamStore.getLatestVersionEntry(procUID, outputName);
        DataStreamKey newDsKey;
        IDataStreamInfo newDsInfo;
        
        if (dsEntry == null)
        {
            // create new data stream
            newDsInfo = new DataStreamInfo.Builder()
                .withProcedure(new ProcedureId(procKey.getInternalID(), procUID))
                .withRecordDescription(dataStruct)
                .withRecordEncoding(dataEncoding)
                .build();
            newDsKey = dataStreamStore.add(newDsInfo);
            
            // send event        
            getEventPublisher().publish(new DataStreamAddedEvent(procUID, outputName));
            
            log.debug("Added new datastream {}#{}", procUID, outputName);
        }
        else
        {
            // if an output with the same name already existed
            newDsKey = dsEntry.getKey();
            IDataStreamInfo oldDsInfo = dsEntry.getValue();
            
            // check if datastream already has observations
            var hasObs = oldDsInfo.getResultTimeRange() != null;
            
            // 2 cases
            // if structure has changed, create a new datastream
            if (hasObs &&
               (!DataComponentChecks.checkStructCompatible(oldDsInfo.getRecordStructure(), dataStruct) ||
                !DataComponentChecks.checkEncodingEquals(oldDsInfo.getRecordEncoding(), dataEncoding)))
            {
                newDsInfo = new DataStreamInfo.Builder()
                    .withProcedure(new ProcedureId(procKey.getInternalID(), procUID))
                    .withRecordDescription(dataStruct)
                    .withRecordEncoding(dataEncoding)
                    .build();
                
                newDsKey = dataStreamStore.add(newDsInfo);
                getEventPublisher().publish(new DataStreamAddedEvent(procUID, outputName));
                
                log.debug("Created new version of datastream {}#{}", procUID, outputName);
            }
            
            // if something else has changed, update existing datastream
            else if (!DataComponentChecks.checkStructEquals(oldDsInfo.getRecordStructure(), dataStruct) ||
                     !DataComponentChecks.checkEncodingEquals(oldDsInfo.getRecordEncoding(), dataEncoding))
            {
                newDsInfo = new DataStreamInfo.Builder()
                    .withProcedure(new ProcedureId(procKey.getInternalID(), procUID))
                    .withRecordDescription(dataStruct)
                    .withRecordEncoding(dataEncoding)
                    .withValidTime(oldDsInfo.getValidTime())
                    .build();
                
                dataStreamStore.put(newDsKey, newDsInfo);
                getEventPublisher().publish(new DataStreamChangedEvent(procUID, outputName));
                
                log.debug("Updated datastream {}#{}", procUID, outputName);
            }
            
            // else don't update and return existing key
            else
            {
                newDsInfo = oldDsInfo;
                log.debug("No changes to datastream {}#{}", procUID, outputName);
            }
        }
        
        // create the new procedure handler
        return new DataStreamTransactionHandler(newDsKey, newDsInfo, foiIdMap, rootHandler);
    }
    
    
    public synchronized boolean deleteDataStream(String outputName)
    {
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        disableDataStream(outputName);
        
        var isDeleted = getDataStreamStore().removeAllVersions(procUID, outputName) > 0;
        
        if (isDeleted)
            getEventPublisher().publish(new DataStreamRemovedEvent(procUID, outputName));
        
        return isDeleted;
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
    
    
    /*
     * FOI helper methods
     */    
    
    public synchronized FeatureKey addOrUpdateFoi(IGeoFeature foi) throws DataStoreException
    {
        DataStoreUtils.checkFeatureObject(foi);
        
        var uid = foi.getUniqueIdentifier();
        boolean isNew = true;
        
        FeatureKey fk = rootHandler.db.getFoiStore().getCurrentVersionKey(uid);
        
        // store feature description if none was found
        if (fk == null)
            fk = getFoiStore().add(procKey.getInternalID(), foi);
        
        // otherwise add it only if its newer than the one already in storage
        else
        {
            var validTime = foi instanceof ITemporalFeature ? ((ITemporalFeature)foi).getValidTime() : null;
            if (validTime != null && fk.getValidStartTime().isBefore(validTime.begin()))
                fk = getFoiStore().add(procKey.getInternalID(), foi);
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
        
        return fk;
    }
    
    
    protected IProcedureStore getProcedureStore()
    {
        return rootHandler.db.getProcedureStore();
    }
    
    
    protected IDataStreamStore getDataStreamStore()
    {
        return rootHandler.db.getDataStreamStore();
    }
    
    
    protected IFoiStore getFoiStore()
    {
        return rootHandler.db.getFoiStore();
    }
    
    
    protected IEventPublisher getEventPublisher()
    {
        var eventSrcInfo = EventUtils.getProcedureEventSourceInfo(parentGroupUID, procUID);
        return rootHandler.eventBus.getPublisher(eventSrcInfo);
    }
    
    
    protected IEventPublisher getParentPublisher()
    {
        if (parentGroupUID != null)
        {
            var eventSrcInfo = EventUtils.getProcedureEventSourceInfo(parentGroupUID, procUID);
            return rootHandler.eventBus.getPublisher(eventSrcInfo);
        }
        
        return rootHandler.eventBus.getPublisher(ISensorHub.EVENT_SOURCE_INFO);
    }
    
    
    protected void checkParent()
    {
        if (parentGroupUID == null)
        {
            var parentID = getProcedureStore().getParent(procKey.getInternalID());
            if (parentID > 0)
                parentGroupUID = getProcedureStore().getCurrentVersion(parentID).getUniqueIdentifier();
        }
    }
    
    
    protected ProcedureTransactionHandler createMemberProcedureHandler(FeatureKey memberKey, String memberUID)
    {
        return new ProcedureTransactionHandler(memberKey, memberUID, procUID, rootHandler);
    }
    
    
    public FeatureKey getProcedureKey()
    {
        return procKey;
    }


    public boolean isNew()
    {
        return newlyCreated;
    }
}
