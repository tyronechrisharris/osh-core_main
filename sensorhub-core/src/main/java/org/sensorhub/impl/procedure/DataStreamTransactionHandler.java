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
import java.util.HashMap;
import java.util.Map;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.DataStreamChangedEvent;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.DataStreamRemovedEvent;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsData;
import org.sensorhub.utils.SWEDataUtils;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
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
    protected IEventPublisher eventPublisher;
    protected ScalarIndexer timeStampIndexer;
    protected Map<String, FeatureId> foiIdMap;
    
    
    public DataStreamTransactionHandler(DataStreamKey dsKey, IDataStreamInfo dsInfo, ProcedureObsTransactionHandler rootHandler)
    {
        this(dsKey, dsInfo, new HashMap<>(), rootHandler);
    }
    
    
    public DataStreamTransactionHandler(DataStreamKey dsKey, IDataStreamInfo dsInfo, Map<String, FeatureId> foiIdMap, ProcedureObsTransactionHandler rootHandler)
    {
        this.dsKey = dsKey;
        this.dsInfo = dsInfo;
        this.timeStampIndexer = SWEHelper.getTimeStampIndexer(dsInfo.getRecordStructure());
        this.rootHandler = rootHandler;
        this.foiIdMap = Asserts.checkNotNull(foiIdMap, "foiIdMap");
    }
    
    
    public boolean update(DataComponent dataStruct, DataEncoding dataEncoding)
    {
        var newDsInfo = new DataStreamInfo.Builder()
            .withProcedure(dsInfo.getProcedureID())
            .withRecordDescription(dataStruct)
            .withRecordEncoding(dataEncoding)
            .build();
        
        var oldDsInfo = getDataStreamStore().replace(dsKey, newDsInfo);
        if (oldDsInfo == null)
            return false;
        
        this.dsInfo = newDsInfo;
        getEventPublisher().publish(new DataStreamChangedEvent(
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName()));
        
        return true;
    }
    
    
    public boolean delete()
    {
        var oldDsKey = getDataStreamStore().remove(dsKey);
        if (oldDsKey == null)
            return false;
                
        getEventPublisher().publish(new DataStreamRemovedEvent(
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName()));
                
        return true;
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
        getEventPublisher().publish(e);
        
        // if event carries an FOI UID, try to fetch the full Id object
        FeatureId foiId;
        String foiUID = e.getFoiUID();
        if (foiUID != null)
        {
            var fid = foiIdMap.get(foiUID);
            if (fid != null)
                foiId = fid;
            else
                throw new IllegalStateException("Unknown FOI: " + foiUID);
        }
        
        // else use the single FOI if there is one
        else if (foiIdMap.size() == 1)
        {
            foiId = foiIdMap.values().iterator().next();
        }
        
        // else don't associate to any FOI
        else
            foiId = ObsData.NO_FOI;
        
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
        
        // first send to event bus to minimize latency
        var foiID = obs.getFoiID();
        getEventPublisher().publish(new DataEvent(
            System.currentTimeMillis(),
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName(),
            foiID == null || foiID == FeatureId.NULL_FEATURE ? null : foiID.getUniqueID(),
            obs.getResult()));        
        
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
        
        
    protected synchronized IEventPublisher getEventPublisher()
    {
        // create event publisher if needed
        if (eventPublisher == null)
        {
            var procID = dsInfo.getProcedureID();
            
            // fetch parent UID if needed
            if (parentGroupUID == null)
            {
                var parentID = rootHandler.db.getProcedureStore().getParent(procID.getInternalID());
                if (parentID > 0)
                    parentGroupUID = rootHandler.db.getProcedureStore().getCurrentVersion(parentID).getUniqueIdentifier();
            }
            
            var eventSrcInfo = EventUtils.getOutputEventSourceInfo(parentGroupUID, procID.getUniqueID(), dsInfo.getOutputName());
            eventPublisher = rootHandler.eventBus.getPublisher(eventSrcInfo);
        }
         
        return eventPublisher;
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
