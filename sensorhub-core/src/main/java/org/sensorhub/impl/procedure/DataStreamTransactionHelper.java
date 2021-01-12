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
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsData;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.utils.DataComponentChecks;
import org.sensorhub.utils.SWEDataUtils;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Helper class for creating/updating/deleting datastreams and persisting 
 * observations in the associated datastore, as well as publishing the
 * corresponding events.
 * </p>
 *
 * @author Alex Robin
 * @date Dec 21, 2020
 */
public class DataStreamTransactionHelper implements IEventListener
{
    final protected ISensorHub hub;
    final protected IObsStore obsStore;
    
    protected long dataStreamID;
    protected String procUID;
    protected String outputName;
    protected String parentGroupUID;
    protected IEventPublisher eventPublisher;
    protected ScalarIndexer timeStampIndexer;
    protected Map<String, FeatureId> featureIdMap;
    
    
    public DataStreamTransactionHelper(ISensorHub hub, IObsStore obsStore)
    {
        this(hub, obsStore, Collections.emptyMap());
    }
    
    
    public DataStreamTransactionHelper(ISensorHub hub, IObsStore obsStore, Map<String, FeatureId> featureIdMap)
    {
        this.hub = Asserts.checkNotNull(hub, ISensorHub.class);
        this.obsStore = Asserts.checkNotNull(obsStore, IObsStore.class);
        this.featureIdMap = Asserts.checkNotNull(featureIdMap, "featureIdMap");
    }    
    
    
    /*
     * Non-static methods to be used when caching an instance of this class
     */    
    
    protected IEventPublisher getEventPublisher()
    {
        if (eventPublisher == null)
        {
            var eventSrcInfo = EventUtils.getOutputEventSourceInfo(parentGroupUID, procUID, outputName);
            eventPublisher = hub.getEventBus().getPublisher(eventSrcInfo);
        }
        
        return eventPublisher;
    }
    
    
    /**
     * Init handler with known datastream info
     * @param dsID
     * @param dsInfo
     */
    public void init(long dsID, IDataStreamInfo dsInfo)
    {
        // cache info in this class
        dataStreamID = dsID;
        procUID = dsInfo.getProcedureID().getUniqueID();
        outputName = dsInfo.getOutputName();
        timeStampIndexer = SWEHelper.getTimeStampIndexer(dsInfo.getRecordStructure());
    }
    
    
    /**
     * Connect to an existing datastream with the specified internal ID
     * @param id Datastream internal ID
     * @return True if datastream was found, false otherwise 
     */
    public boolean connect(long id)
    {
        OshAsserts.checkValidInternalID(id);
        
        // check if it was already initialized
        if (this.dataStreamID != 0)
        {
            if (this.dataStreamID != id)
                throw new IllegalStateException("Internal IDs don't match");
            return true;
        }
        
        // load datastream info from DB
        var dsInfo = obsStore.getDataStreams().get(new DataStreamKey(id));
        if (dsInfo == null)
            return false;
        
        init(id, dsInfo);
        return true;
    }
    
    
    /**
     * Connect to an existing datastream with the specified procedure and output.
     * This connects to the currently valid datastream attached to the given output.
     * @param procUID Procedure unique ID
     * @param outputName Output name
     * @return True if datastream was found, false otherwise 
     */
    public boolean connect(String procUID, String outputName)
    {
        OshAsserts.checkValidUID(procUID);
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        
        // load datastream info from DB
        var dsEntry = obsStore.getDataStreams().getLatestVersionEntry(procUID, outputName);
        if (dsEntry == null)
            return false;
        
        // check if it was already initialized
        if (this.dataStreamID != 0)
        {
            if (this.dataStreamID != dsEntry.getKey().getInternalID())
                throw new IllegalStateException("Internal IDs don't match");
            return true;
        }
        
        // cache info in this class
        init(dsEntry.getKey().getInternalID(), dsEntry.getValue());
        return true;
    }
    
    
    public void update(IDataStreamInfo dsInfo)
    {
        checkInitialized();
        
        // detect if structure has really changed
    }
    
    
    public void addObs(DataBlock rec)
    {
        // no checks since this is called at high rate
        //Asserts.checkNotNull(rec, DataBlock.class);
        
        addObs(new DataEvent(
            System.currentTimeMillis(), procUID, outputName, rec));
    }
    
    
    public void addObs(DataEvent e)
    {
        // no checks since this is called at high rate
        //Asserts.checkNotNull(e, DataEvent.class);
        //checkInitialized();
        
        // first forward to event bus to minimize latency
        getEventPublisher().publish(e);
        
        // if event carries an FOI UID, try to fetch the full Id object
        FeatureId foiId;
        String foiUID = e.getFoiUID();
        if (foiUID != null)
        {
            var fid = featureIdMap.get(foiUID);
            if (fid != null)
                foiId = fid;
            else
                throw new IllegalStateException("Unknown FOI: " + foiUID);
        }
        
        // else use the single FOI if there is one
        else if (featureIdMap.size() == 1)
        {
            foiId = featureIdMap.values().iterator().next();
        }
        
        // else don't associate to any FOI
        else
            foiId = ObsData.NO_FOI;
        
        // process all records
        for (DataBlock record: e.getRecords())
        {
            // get time stamp
            double time;
            if (timeStampIndexer != null)
                time = timeStampIndexer.getDoubleValue(record);
            else
                time = e.getTimeStamp() / 1000.;
        
            // store record with proper key
            ObsData obs = new ObsData.Builder()
                .withDataStream(dataStreamID)
                .withFoi(foiId)
                .withPhenomenonTime(SWEDataUtils.toInstant(time))
                .withResult(record)
                .build();
            
            // add to store
            obsStore.add(obs);
        }
    }
    
    
    public void addObs(IObsData obs)
    {
        // no checks since this is called at high rate
        //Asserts.checkNotNull(obs, IObsData.class);
        //checkInitialized();
        
        // first send to event bus to minimize latency
        getEventPublisher().publish(new DataEvent(
            System.currentTimeMillis(), procUID, outputName,
            obs.getFoiID() == FeatureId.NULL_FEATURE ? null : obs.getFoiID().getUniqueID(),
            obs.getResult()));        
        
        // add to store
        obsStore.add(obs);
    }


    @Override
    public void handleEvent(Event e)
    {
        if (e instanceof DataEvent)
        {
            addObs((DataEvent)e);
        }
    }
        

    /*
     * Methods called internally by ProcedureTransactionHandler
     */
    
    protected boolean createOrUpdate(ProcedureId procId, String outputName, DataComponent dataStruct, DataEncoding dataEncoding) throws DataStoreException
    {
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        Asserts.checkNotNull(dataStruct, DataComponent.class);
        Asserts.checkNotNull(dataEncoding, DataEncoding.class);
        
        if (dataStruct.getName() == null)
            dataStruct.setName(outputName);
        else if (!outputName.equals(dataStruct.getName()))
            throw new IllegalStateException("Inconsistent output name");
        
        // try to retrieve existing data stream
        IDataStreamStore dataStreamStore = obsStore.getDataStreams();
        Entry<DataStreamKey, IDataStreamInfo> dsEntry = dataStreamStore.getLatestVersionEntry(procId.getUniqueID(), outputName);
        DataStreamKey newDsKey;
        IDataStreamInfo newDsInfo;
        boolean isNew = true;
        
        if (dsEntry == null)
        {
            // create new data stream
            newDsInfo = new DataStreamInfo.Builder()
                .withProcedure(procId)
                .withRecordDescription(dataStruct)
                .withRecordEncoding(dataEncoding)
                .build();
            newDsKey = dataStreamStore.add(newDsInfo);
        }
        else
        {
            // if an output with the same name already existed
            newDsKey = dsEntry.getKey();
            IDataStreamInfo oldDsInfo = dsEntry.getValue();
            
            newDsInfo = new DataStreamInfo.Builder()
                .withProcedure(procId)
                .withRecordDescription(dataStruct)
                .withRecordEncoding(dataEncoding)
                .withValidTime(TimeExtent.endNow(Instant.now()))
                .build();
            
            // 2 cases
            // if structure has changed, create a new datastream
            if (!DataComponentChecks.checkStructCompatible(oldDsInfo.getRecordStructure(), newDsInfo.getRecordStructure()))
                newDsKey = dataStreamStore.add(newDsInfo);
            
            // if something else has changed, update existing datastream
            else if (!DataComponentChecks.checkStructEquals(oldDsInfo.getRecordStructure(), newDsInfo.getRecordStructure()))
                dataStreamStore.put(newDsKey, newDsInfo); 
            
            // else don't update and return existing key
            else
                isNew = false;
        }
        
        init(newDsKey.getInternalID(), newDsInfo);
        return isNew;
    }
    
    
    protected void checkInitialized()
    {
        Asserts.checkState(dataStreamID > 0 && procUID != null && outputName != null, "datastream handler is not initialized");
    }
    
    
    public long getDataStreamID()
    {
        return dataStreamID;
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
