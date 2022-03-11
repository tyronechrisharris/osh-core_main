/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.system;

import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import org.sensorhub.api.command.CommandStreamAddedEvent;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.data.DataStreamAddedEvent;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.feature.FoiAddedEvent;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.api.system.SystemAddedEvent;
import org.sensorhub.api.system.SystemChangedEvent;
import org.sensorhub.api.system.SystemId;
import org.sensorhub.api.system.SystemRemovedEvent;
import org.sensorhub.api.system.SystemDisabledEvent;
import org.sensorhub.api.system.SystemEnabledEvent;
import org.sensorhub.api.system.SystemEvent;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.utils.DataComponentChecks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Asserts;
import com.google.common.base.Strings;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Helper class for updating the state of a specific observing system.
 * This class handles database transactions and generate proper events when
 * system components are added, removed or modified.
 * </p>
 *
 * @author Alex Robin
 * @since Oct 4, 2021
 */
public class SystemTransactionHandler
{
    static final Logger log = LoggerFactory.getLogger(SystemTransactionHandler.class);
    
    protected final SystemDatabaseTransactionHandler rootHandler;
    protected final String sysUID;
    protected FeatureKey sysKey;
    protected String parentGroupUID;
    protected Map<String, Long> foiIdMap;
    protected boolean newlyCreated;
    
    
    public SystemTransactionHandler(FeatureKey sysKey, String sysUID, SystemDatabaseTransactionHandler rootHandler)
    {
        this(sysKey, sysUID, null, rootHandler);
    }
    
    
    public SystemTransactionHandler(FeatureKey sysKey, String sysUID, String parentGroupUID, SystemDatabaseTransactionHandler rootHandler)
    {
        this.sysKey = Asserts.checkNotNull(sysKey, FeatureKey.class);
        this.sysUID = OshAsserts.checkValidUID(sysUID);
        this.parentGroupUID = parentGroupUID;
        this.rootHandler = Asserts.checkNotNull(rootHandler);
        
        // prepare lazy loaded map of FOI UID to full FeatureId
        this.foiIdMap = rootHandler.createFoiIdCache();
    }
    
    
    public synchronized boolean update(ISystemWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        if (!getSystemDescStore().contains(proc.getUniqueIdentifier()))
            return false;
        
        var validTime = proc.getFullDescription().getValidTime();
        if (validTime == null || sysKey.getValidStartTime().isBefore(validTime.begin()))
        {
            sysKey = getSystemDescStore().add(proc);
            publishSystemEvent(new SystemChangedEvent(sysUID));
        }
        else if (sysKey.getValidStartTime().equals(validTime.begin()))
        {
            getSystemDescStore().put(sysKey, proc);
        }
        else
            throw new DataStoreException("A version of the system description with a more recent valid time already exists");
        
        return true;
    }
    
    
    public boolean delete() throws DataStoreException
    {
        // check if we have a parent
        checkParent();
        
        // error if associated datastreams still exist
        var procDsFilter = new DataStreamFilter.Builder()
            .withSystems(sysKey.getInternalID())
            .build();
        if (getDataStreamStore().countMatchingEntries(procDsFilter) > 0)
            throw new DataStoreException("System cannot be deleted because it is referenced by a datastream");
        
        // remove from datastore
        var procKey = getSystemDescStore().remove(sysUID);
        if (procKey != null)
        {
            // send deleted event
            publishSystemEvent(new SystemRemovedEvent(sysUID, parentGroupUID));
            return true;
        }
        
        return false;
    }
    
    
    public void enable()
    {
        checkParent();
        publishSystemEvent(new SystemEnabledEvent(sysUID, parentGroupUID));
    }
    
    
    public void disable()
    {
        checkParent();
        publishSystemEvent(new SystemDisabledEvent(sysUID, parentGroupUID));
    }
    
    
    
    /*
     * Member helper methods
     */    
    
    public synchronized SystemTransactionHandler addMember(ISystemWithDesc proc) throws DataStoreException
    {
        OshAsserts.checkProcedureObject(proc);
        
        var parentGroupUID = this.sysUID;
        var parentGroupID = this.sysKey.getInternalID();
        
        // add to datastore
        var memberKey = getSystemDescStore().add(parentGroupID, proc);
        var memberUID = proc.getUniqueIdentifier();
        
        // send event
        publishSystemEvent(new SystemAddedEvent(memberUID, parentGroupUID));
        
        // create the new system handler
        return createMemberHandler(memberKey, memberUID);
    }
    
    
    public synchronized SystemTransactionHandler addOrUpdateMember(ISystemWithDesc proc) throws DataStoreException
    {
        var uid = OshAsserts.checkProcedureObject(proc);
        
        var memberKey = getSystemDescStore().getCurrentVersionKey(uid);
        if (memberKey != null)
        {
            var memberHandler = createMemberHandler(memberKey, uid);
            memberHandler.update(proc);
            return memberHandler;
        }
        else
            return addMember(proc);
    }
    
    
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
        IDataStreamStore dataStreamStore = getDataStreamStore();
        Entry<DataStreamKey, IDataStreamInfo> dsEntry = dataStreamStore.getLatestVersionEntry(sysUID, outputName);
        DataStreamKey dsKey;
        IDataStreamInfo newDsInfo;
        DataStreamAddedEvent addedEvent = null;
        
        if (dsEntry == null)
        {
            // retrieve parent system name
            var procName = getSystemDescStore().get(sysKey).getName();
            var dsName = Strings.isNullOrEmpty(dataStruct.getLabel()) ? outputName : dataStruct.getLabel();
                
            // create new data stream
            newDsInfo = new DataStreamInfo.Builder()
                .withName(procName + " - " + dsName)
                .withSystem(new SystemId(sysKey.getInternalID(), sysUID))
                .withRecordDescription(dataStruct)
                .withRecordEncoding(dataEncoding)
                .build();
            dsKey = dataStreamStore.add(newDsInfo);
            
            // send event
            addedEvent = new DataStreamAddedEvent(sysUID, outputName);
            log.debug("Added new datastream {}#{}", sysUID, outputName);
        }
        else
        {
            // if an output with the same name already exists
            dsKey = dsEntry.getKey();
            IDataStreamInfo oldDsInfo = dsEntry.getValue();
            
            // check if datastream already has observations
            var hasObs = oldDsInfo.getResultTimeRange() != null;
            
            // 3 cases
            // if observations were already recorded and structure has changed, create a new datastream
            if (hasObs &&
               (!DataComponentChecks.checkStructCompatible(oldDsInfo.getRecordStructure(), dataStruct) ||
                !DataComponentChecks.checkEncodingEquals(oldDsInfo.getRecordEncoding(), dataEncoding)))
            {
                newDsInfo = new DataStreamInfo.Builder()
                    .withName(oldDsInfo.getName())
                    .withSystem(new SystemId(sysKey.getInternalID(), sysUID))
                    .withRecordDescription(dataStruct)
                    .withRecordEncoding(dataEncoding)
                    .build();
                
                dsKey = dataStreamStore.add(newDsInfo);
                addedEvent = new DataStreamAddedEvent(sysUID, outputName);
                log.debug("Created new version of datastream {}#{}", sysUID, outputName);
            }
            
            // if something else has changed, update existing datastream
            else if (!DataComponentChecks.checkStructEquals(oldDsInfo.getRecordStructure(), dataStruct) ||
                     !DataComponentChecks.checkEncodingEquals(oldDsInfo.getRecordEncoding(), dataEncoding))
            {
                var dsHandler = new DataStreamTransactionHandler(dsKey, oldDsInfo, rootHandler);
                dsHandler.update(dataStruct, dataEncoding);
                newDsInfo = dsHandler.getDataStreamInfo();
                log.debug("Updated datastream {}#{}", sysUID, outputName);
            }
            
            // else don't update and return existing key
            else
            {
                newDsInfo = oldDsInfo;
                log.debug("No changes to datastream {}#{}", sysUID, outputName);
            }
        }
        
        // create the new datastream handler
        var dsHandler = new DataStreamTransactionHandler(dsKey, newDsInfo, foiIdMap, rootHandler);
        if (addedEvent != null)
            dsHandler.publishDataStreamEvent(addedEvent);
        return dsHandler;
    }
    
    
    public synchronized CommandStreamTransactionHandler addOrUpdateCommandStream(String commandName, DataComponent dataStruct, DataEncoding dataEncoding) throws DataStoreException
    {
        Asserts.checkNotNullOrEmpty(commandName, "commandName");
        Asserts.checkNotNull(dataStruct, DataComponent.class);
        Asserts.checkNotNull(dataEncoding, DataEncoding.class);
        
        // check command name == data component name
        if (dataStruct.getName() == null)
            dataStruct.setName(commandName);
        else if (!commandName.equals(dataStruct.getName()))
            throw new IllegalStateException("Inconsistent command name");
        
        // try to retrieve existing command stream
        ICommandStreamStore commandStreamStore = getCommandStreamStore();
        Entry<CommandStreamKey, ICommandStreamInfo> csEntry = commandStreamStore.getLatestVersionEntry(sysUID, commandName);
        CommandStreamKey csKey;
        ICommandStreamInfo newCsInfo;
        CommandStreamAddedEvent addedEvent = null;
        
        if (csEntry == null)
        {
            // retrieve parent system name
            var procName = getSystemDescStore().get(sysKey).getName();
            var csName = Strings.isNullOrEmpty(dataStruct.getLabel()) ? commandName : dataStruct.getLabel();
                
            // create new command stream
            newCsInfo = new CommandStreamInfo.Builder()
                .withName(procName + " - " + csName)
                .withSystem(new SystemId(sysKey.getInternalID(), sysUID))
                .withRecordDescription(dataStruct)
                .withRecordEncoding(dataEncoding)
                .build();
            csKey = commandStreamStore.add(newCsInfo);
            
            // send event
            addedEvent = new CommandStreamAddedEvent(sysUID, commandName);
            log.debug("Added new command stream {}#{}", sysUID, commandName);
        }
        else
        {
            // if a command input with the same name already exists
            csKey = csEntry.getKey();
            ICommandStreamInfo oldCsInfo = csEntry.getValue();
            
            // check if command stream already has commands
            var hasCommands = oldCsInfo.getIssueTimeRange() != null;
            
            // 3 cases
            // if commands were already recorded and structure has changed, create a new command stream
            if (hasCommands &&
               (!DataComponentChecks.checkStructCompatible(oldCsInfo.getRecordStructure(), dataStruct) ||
                !DataComponentChecks.checkEncodingEquals(oldCsInfo.getRecordEncoding(), dataEncoding)))
            {
                newCsInfo = new CommandStreamInfo.Builder()
                    .withName(oldCsInfo.getName())
                    .withSystem(new SystemId(sysKey.getInternalID(), sysUID))
                    .withRecordDescription(dataStruct)
                    .withRecordEncoding(dataEncoding)
                    .build();
                
                csKey = commandStreamStore.add(newCsInfo);
                addedEvent = new CommandStreamAddedEvent(sysUID, commandName);
                log.debug("Created new version of command stream {}#{}", sysUID, commandName);
            }
            
            // if something else has changed, update existing command stream
            else if (!DataComponentChecks.checkStructEquals(oldCsInfo.getRecordStructure(), dataStruct) ||
                     !DataComponentChecks.checkEncodingEquals(oldCsInfo.getRecordEncoding(), dataEncoding))
            {
                var csHandler = new CommandStreamTransactionHandler(csKey, oldCsInfo, rootHandler);
                csHandler.update(dataStruct, dataEncoding);
                newCsInfo = csHandler.getCommandStreamInfo();
                log.debug("Updated command stream {}#{}", sysUID, commandName);
            }
            
            // else don't update and return existing key
            else
            {
                newCsInfo = oldCsInfo;
                log.debug("No changes to command stream {}#{}", sysUID, commandName);
            }
        }
        
        // create the new command stream handler
        var csHandler = new CommandStreamTransactionHandler(csKey, newCsInfo, rootHandler);
        if (addedEvent != null)
            csHandler.publishCommandStreamEvent(addedEvent);
        return csHandler;
    }
    
    
    public synchronized FeatureKey addOrUpdateFoi(IFeature foi) throws DataStoreException
    {
        DataStoreUtils.checkFeatureObject(foi);
        
        var uid = foi.getUniqueIdentifier();
        boolean isNew;
        
        FeatureKey fk = rootHandler.db.getFoiStore().getCurrentVersionKey(uid);
        
        // store feature description if none was found
        if (fk == null)
        {
            fk = getFoiStore().add(sysKey.getInternalID(), foi);
            isNew = true;
            log.debug("Added FOI {}", foi.getUniqueIdentifier());
            
        }
        
        // otherwise add it only if its newer than the one already in storage
        // otherwise update existing one
        else
        {
            if (foi.getValidTime() != null && fk.getValidStartTime().isBefore(foi.getValidTime().begin()))
            {
                fk = getFoiStore().add(sysKey.getInternalID(), foi);
                isNew = true;
                log.debug("Added FOI {}", foi.getUniqueIdentifier());
            }
            else
            {
                getFoiStore().put(fk, foi);
                isNew = false;
                log.debug("Updated FOI {}", foi.getUniqueIdentifier());
            }
        }
        
        foiIdMap.put(uid, fk.getInternalID());
        
        if (isNew)
        {
            publishSystemEvent(new FoiAddedEvent(
                System.currentTimeMillis(),
                sysUID,
                foi.getUniqueIdentifier(),
                Instant.now()));
        }
        
        return fk;
    }
    
    
    protected ISystemDescStore getSystemDescStore()
    {
        return rootHandler.db.getSystemDescStore();
    }
    
    
    protected IDataStreamStore getDataStreamStore()
    {
        return rootHandler.db.getDataStreamStore();
    }
    
    
    protected ICommandStreamStore getCommandStreamStore()
    {
        return rootHandler.db.getCommandStreamStore();
    }
    
    
    protected IFoiStore getFoiStore()
    {
        return rootHandler.db.getFoiStore();
    }
    
    
    protected void publishSystemEvent(SystemEvent event)
    {
        String topic;
        
        // assign internal ID before event is dispatched
        event.assignSystemID(sysKey.getInternalID());
        
        // publish on system status channel
        topic = EventUtils.getSystemStatusTopicID(sysUID);
        rootHandler.eventBus.getPublisher(topic).publish(event);
        
        // publish on parent systems status recursively
        long sysId = rootHandler.db.getSystemDescStore().getCurrentVersionKey(sysUID).getInternalID();
        Long parentId = sysId != 0 ? sysId : null;
        while ((parentId = rootHandler.db.getSystemDescStore().getParent(parentId)) != null)
        {
            var sysUid = rootHandler.db.getSystemDescStore().getCurrentVersion(parentId).getUniqueIdentifier();
            topic = EventUtils.getSystemStatusTopicID(sysUid);
            rootHandler.eventBus.getPublisher(topic).publish(event);
        }
        
        // publish on systems root
        topic = EventUtils.getSystemRegistryTopicID();
        rootHandler.eventBus.getPublisher(topic).publish(event);
    }
    
    
    protected void checkParent()
    {
        if (parentGroupUID == null)
        {
            var parentID = getSystemDescStore().getParent(sysKey.getInternalID());
            if (parentID != null && parentID > 0)
                parentGroupUID = getSystemDescStore().getCurrentVersion(parentID).getUniqueIdentifier();
        }
    }
    
    
    protected SystemTransactionHandler createMemberHandler(FeatureKey memberKey, String memberUID)
    {
        return new SystemTransactionHandler(memberKey, memberUID, sysUID, rootHandler);
    }
    
    
    public FeatureKey getSystemKey()
    {
        return sysKey;
    }
    
    
    public String getSystemUID()
    {
        return sysUID;
    }


    public boolean isNew()
    {
        return newlyCreated;
    }
}
