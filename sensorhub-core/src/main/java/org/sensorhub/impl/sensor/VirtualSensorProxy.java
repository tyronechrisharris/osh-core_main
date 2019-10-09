/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor;

import java.lang.ref.WeakReference;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.sensor.SensorException;
import org.sensorhub.impl.event.EventSourceInfo;
import org.sensorhub.utils.DataStructureHash;
import org.vast.ogc.gml.FeatureRef;
import org.vast.util.Asserts;
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.ByteEncoding;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Proxy used by services generating virtual sensors (e.g. SOS-T, SensorThings)
 * </p>
 *
 * @author Alex Robin
 * @date Sep 10, 2019
 */
public class VirtualSensorProxy extends SensorShadow
{
    private static final long serialVersionUID = -3281124788006180015L;
    
    
    public VirtualSensorProxy(AbstractProcess desc)
    {
        this(desc, null);
    }
    
    
    public VirtualSensorProxy(AbstractProcess desc, String parentUID)
    {
        this.latestDescription = Asserts.checkNotNull(desc, AbstractProcess.class);
        this.lastDescriptionUpdate = System.currentTimeMillis();
        this.ref = new WeakReference<>(null);
        this.parentUID = parentUID;
        
        // create event source
        String groupID = Asserts.checkNotNull(getUniqueIdentifier(), "uniqueID");
        String sourceID = EventUtils.getProcedureSourceID(groupID);
        this.eventSrcInfo = new EventSourceInfo(groupID, sourceID);
    }
    
    
    @Override
    public boolean isEnabled()
    {
        return eventPublisher != null;
    }
    
    
    public void connect()
    {
        if (registry != null && eventPublisher == null)
        {
            eventPublisher = registry.getParentHub().getEventBus().getPublisher(eventSrcInfo);
            
            for (IStreamingDataInterface output: getOutputs().values())
                ((VirtualOutputProxy)output).createPublisher();
        }
    }
    
    
    public synchronized void updateDescription(AbstractProcess desc)
    {
        long now = this.lastDescriptionUpdate = System.currentTimeMillis();
        this.latestDescription = desc;
                
        if (registry != null)
        {
            if (eventPublisher != null)
                eventPublisher.publish(new ProcedureChangedEvent(now, desc.getUniqueIdentifier()));
        }
    }
    
    
    public synchronized void newOutput(DataComponent recordStruct, DataEncoding encoding)
    {
        Asserts.checkNotNull(recordStruct, DataComponent.class);
        Asserts.checkNotNull(encoding, DataEncoding.class);
        boolean outputChanged = true;        
        
        // if output already exists, check that it has really changed
        String outputName = recordStruct.getName();
        if (obsOutputs.containsKey(outputName))
        {
            OutputProxy existingOutput = obsOutputs.get(outputName);
            DataStructureHash oldOutputHashObj = new DataStructureHash(existingOutput.getRecordDescription(), existingOutput.getRecommendedEncoding());
            DataStructureHash newOutputHashObj = new DataStructureHash(recordStruct, encoding);
            outputChanged = !newOutputHashObj.equals(oldOutputHashObj);                
        }
        
        // create new sensor output only if it has changed
        if (outputChanged)
        {
            VirtualOutputProxy newOutput = new VirtualOutputProxy(recordStruct, encoding);
            obsOutputs.put(recordStruct.getName(), newOutput);
            
            // rebuild output list in SensorML description
            latestDescription.getOutputList().clear();
            for (OutputProxy output: obsOutputs.values())
                latestDescription.getOutputList().add(output.getName(), output.getRecordDescription());
            
            updateDescription(latestDescription);
        }
    }
    
    
    public synchronized void newControlInput(DataComponent recordStruct, DataEncoding encoding) throws SensorException
    {
        Asserts.checkNotNull(recordStruct, DataComponent.class);
        Asserts.checkNotNull(encoding, DataEncoding.class);
        boolean inputChanged = true;        
        
        // if output already exists, check that it has really changed
        String outputName = recordStruct.getName();
        if (obsOutputs.containsKey(outputName))
        {
            OutputProxy existingOutput = obsOutputs.get(outputName);
            DataStructureHash oldOutputHashObj = new DataStructureHash(existingOutput.getRecordDescription(), existingOutput.getRecommendedEncoding());
            DataStructureHash newOutputHashObj = new DataStructureHash(recordStruct, encoding);
            inputChanged = !newOutputHashObj.equals(oldOutputHashObj);                
        }
        
        // create new sensor output only if it has changed
        if (inputChanged)
        {
            VirtualOutputProxy newOutput = new VirtualOutputProxy(recordStruct, encoding);
            obsOutputs.put(recordStruct.getName(), newOutput);
            
            // rebuild output list in SensorML description
            latestDescription.getOutputList().clear();
            for (OutputProxy output: obsOutputs.values())
                latestDescription.getOutputList().add(output.getRecordDescription());
            
            updateDescription(latestDescription);
        }
    }
    
    
    public void publishNewRecord(String outputName, DataBlock... dataBlks)
    {
        VirtualOutputProxy output = (VirtualOutputProxy)getOutputs().get(outputName);
        output.publishNewRecord(dataBlks);
    }
    
    
    public void publishNewFoi(String outputName, AbstractFeature foi, double startTime)
    {
        VirtualOutputProxy output = (VirtualOutputProxy)getOutputs().get(outputName);
        output.publishNewFeatureOfInterest(foi, startTime);
    }
    
    
    public synchronized void delete()
    {
        registry.unregister(this);
    }
    
    
    class VirtualOutputProxy extends OutputProxy
    {
        private static final long serialVersionUID = -8091548431814939282L;
        transient int avgSampleCount = 0;
        
        
        VirtualOutputProxy(DataComponent recordStruct, DataEncoding recordEncoding)
        {
            this.ref = new WeakReference<>(null);
            this.recordDescription = recordStruct;
            this.recommmendedEncoding = recordEncoding;
            
            // force raw binary encoding (no reason to recommend base64)
            // switching to base64 is automatic when writing or parsing from XML
            if (recordEncoding instanceof BinaryEncoding)
                ((BinaryEncoding) recordEncoding).setByteEncoding(ByteEncoding.RAW);
            
            // create event source and publisher
            String groupID = getUniqueIdentifier();
            String sourceID = EventUtils.getProcedureOutputSourceID(groupID, getName());
            this.eventSrcInfo = new EventSourceInfo(groupID, sourceID);
            
            if (registry != null)
                createPublisher();
        }
        
        
        void createPublisher()
        {
            if (eventPublisher == null)
                eventPublisher = registry.getParentHub().getEventBus().getPublisher(eventSrcInfo);
        }
        
        
        synchronized void publishNewRecord(DataBlock... dataBlks)
        {
            this.latestRecord = dataBlks[dataBlks.length-1];
            long now = this.latestRecordTime = System.currentTimeMillis();
            eventPublisher.publish(new DataEvent(now, getUniqueIdentifier(), getName(), dataBlks));
            updateSamplingPeriod(now);
        }
        
        
        synchronized void publishNewFeatureOfInterest(AbstractFeature foi, double startTime)
        {
            if (foi != null)
            {            
                long now = System.currentTimeMillis();
                FoiEvent e = null;
                
                if (foi instanceof FeatureRef)
                    e = new FoiEvent(now, getParentProducer(), ((FeatureRef)foi).getHref(), startTime);
                else
                    e = new FoiEvent(now, getParentProducer(), foi, startTime);
                
                eventPublisher.publish(e);
            }
        }
        
        
        /*
         * Refine sampling period at each new measure received by 
         * incrementally computing dt average for the 100 first records
         */
        protected void updateSamplingPeriod(long timeStamp)
        {
            if (latestRecordTime == Long.MIN_VALUE)
                return;
                    
            if (avgSampleCount < 100)
            {
                if (avgSampleCount == 0)
                    avgSamplingPeriod = 0.0;
                else
                    avgSamplingPeriod *= (double)avgSampleCount / (avgSampleCount+1);
                
                avgSampleCount++;
                avgSamplingPeriod += (timeStamp - latestRecordTime) / 1000.0 / avgSampleCount;
            }
        }
    }
}
