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

import java.math.BigInteger;
import java.util.Map;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.DataStreamChangedEvent;
import org.sensorhub.api.data.DataStreamDisabledEvent;
import org.sensorhub.api.data.DataStreamEnabledEvent;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.DataStreamRemovedEvent;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.data.ObsEvent;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.utils.DataComponentChecks;
import org.sensorhub.utils.SWEDataUtils;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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
public class DataStreamTransactionHandler implements IEventListener
{
    protected final ProcedureObsTransactionHandler rootHandler;
    protected final DataStreamKey dsKey;
    protected IDataStreamInfo dsInfo;
    protected String parentGroupUID;
    protected IEventPublisher dataEventPublisher;
    protected ScalarIndexer timeStampIndexer;
    protected Map<String, Long> foiUidToIdMap;
    protected Map<Long, String> foiIdToUidMap;
    
    
    public DataStreamTransactionHandler(DataStreamKey dsKey, IDataStreamInfo dsInfo, ProcedureObsTransactionHandler rootHandler)
    {
        this(dsKey, dsInfo, HashBiMap.create(), rootHandler);
    }
    
    
    public DataStreamTransactionHandler(DataStreamKey dsKey, IDataStreamInfo dsInfo, BiMap<String, Long> foiIdMap, ProcedureObsTransactionHandler rootHandler)
    {
        this.dsKey = dsKey;
        this.dsInfo = dsInfo;
        this.timeStampIndexer = SWEHelper.getTimeStampIndexer(dsInfo.getRecordStructure());
        this.rootHandler = rootHandler;
        
        Asserts.checkNotNull(foiIdMap, "foiIdMap");
        this.foiUidToIdMap = foiIdMap;
        this.foiIdToUidMap = foiIdMap.inverse();
            
        getDataEventPublisher();
    }
    
    
    public boolean update(DataComponent dataStruct, DataEncoding dataEncoding)
    {
        var oldDsInfo = getDataStreamStore().get(dsKey);
        if (oldDsInfo == null)
            return false;
        
        // check if datastream already has observations
        var hasObs = oldDsInfo.getResultTimeRange() != null;
        if (hasObs &&
            (!DataComponentChecks.checkStructCompatible(oldDsInfo.getRecordStructure(), dataStruct) ||
             !DataComponentChecks.checkEncodingEquals(oldDsInfo.getRecordEncoding(), dataEncoding)))
            throw new IllegalArgumentException("Cannot update the record structure or encoding of a datastream if it already has observations");        
        
        // update datastream info
        var newDsInfo = new DataStreamInfo.Builder()
            .withName(oldDsInfo.getName())
            .withDescription(oldDsInfo.getDescription())
            .withProcedure(dsInfo.getProcedureID())
            .withRecordDescription(dataStruct)
            .withRecordEncoding(dataEncoding)
            .withValidTime(oldDsInfo.getValidTime())
            .build();
        getDataStreamStore().replace(dsKey, newDsInfo);        
        this.dsInfo = newDsInfo;
        
        // send event
        var event = new DataStreamChangedEvent(dsInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
        
        return true;
    }
    
    
    public boolean delete()
    {
        var oldDsKey = getDataStreamStore().remove(dsKey);
        if (oldDsKey == null)
            return false;
        
        // if datastream was currently valid, disable it first
        if (dsInfo.getValidTime().endsNow())
            disable();
        
        // send event
        var event = new DataStreamRemovedEvent(dsInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
        
        return true;
    }
    
    
    public void enable()
    {                
        var event = new DataStreamEnabledEvent(dsInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
    }
    
    
    public void disable()
    {                
        var event = new DataStreamDisabledEvent(dsInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
    }
    
    
    public BigInteger addObs(DataBlock rec)
    {
        // no checks since this is called at high rate
        //Asserts.checkNotNull(rec, DataBlock.class);
        
        return addObs(new DataEvent(
            System.currentTimeMillis(),
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName(),
            rec));
    }
    
    
    /**
     * Add all records attached to the event as observations
     * @param e
     * @return ID of last observation inserted
     */
    public BigInteger addObs(DataEvent e)
    {
        // no checks since this is called at high rate
        //Asserts.checkNotNull(e, DataEvent.class);
        //checkInitialized();
        
        // first forward to event bus to minimize latency
        getDataEventPublisher().publish(e);
        
        // if event carries an FOI UID, try to fetch the full Id object
        Long foiId;
        String foiUID = e.getFoiUID();
        if (foiUID != null)
        {
            foiId = foiUidToIdMap.get(foiUID);
            if (foiId == null)
                throw new IllegalStateException("Unknown FOI: " + foiUID);
        }
        
        // else use the single FOI if there is one
        else if (foiUidToIdMap.size() == 1)
        {
            foiId = foiUidToIdMap.values().iterator().next();
        }
        
        // else don't associate to any FOI
        else
            foiId = IObsData.NO_FOI;
        
        // process all records
        BigInteger obsID = null;
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
                .withDataStream(dsKey.getInternalID())
                .withFoi(foiId)
                .withPhenomenonTime(SWEDataUtils.toInstant(time))
                .withResult(record)
                .build();
            
            getDataEventPublisher().publish(new ObsEvent(
                e.getTimeStamp(),
                e.getProcedureUID(),
                e.getOutputName(),
                obs));
            
            // add to store
            obsID = rootHandler.db.getObservationStore().add(obs);
        }
        
        return obsID;
    }
    
    
    public BigInteger addObs(IObsData obs)
    {
        // no checks since this is called at high rate
        //Asserts.checkNotNull(obs, IObsData.class);
        //checkInitialized();
        var timeStamp = System.currentTimeMillis();
        
        // first send to event bus to minimize latency
        getDataEventPublisher().publish(new DataEvent(
            timeStamp,
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName(),
            obs.hasFoi() ? foiIdToUidMap.get(obs.getFoiID()) : null,
            obs.getResult()));
        
        getDataEventPublisher().publish(new ObsEvent(
            timeStamp,
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName(),
            obs));
        
        // add to store
        return rootHandler.db.getObservationStore().add(obs);
    }


    @Override
    public void handleEvent(Event e)
    {
        if (e instanceof DataEvent)
        {
            addObs((DataEvent)e);
        }
    }
    
    
    protected synchronized IEventPublisher getDataEventPublisher()
    {
        // create event publisher if needed
        // cache it because we need it often
        if (dataEventPublisher == null)
        {
            var topic = EventUtils.getDataStreamDataTopicID(dsInfo);
            dataEventPublisher = rootHandler.eventBus.getPublisher(topic);
        }
         
        return dataEventPublisher;
    }
        
        
    protected synchronized IEventPublisher getStatusEventPublisher()
    {
        var topic = EventUtils.getDataStreamStatusTopicID(dsInfo);
        return rootHandler.eventBus.getPublisher(topic);
    }
    
    
    protected IEventPublisher getProcedureStatusEventPublisher()
    {
        var procUID = dsInfo.getProcedureID().getUniqueID();
        var topic = EventUtils.getProcedureStatusTopicID(procUID);
        return rootHandler.eventBus.getPublisher(topic);
    }
    
    
    protected IDataStreamStore getDataStreamStore()
    {
        return rootHandler.db.getDataStreamStore();
    }
    
    
    public DataStreamKey getDataStreamKey()
    {
        return dsKey;
    }
    
    
    public IDataStreamInfo getDataStreamInfo()
    {
        return dsInfo;
    }
}
