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

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.procedure.IProcedureWithState;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.vast.util.Asserts;
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Proxy class reflecting the latest state of the attached data producer.<br/>
 * @see {@link ProcedureProxy}
 * </p>
 *
 * @author Alex Robin
 * @date Sep 6, 2019
 */
public class DataProducerProxy extends ProcedureProxy implements IDataProducer
{
    private static final long serialVersionUID = -6315464994380209210L;
    
    Map<String, OutputProxy> outputs;
    AbstractFeature currentFoi;


    // needed for deserialization
    protected DataProducerProxy()
    {
    }


    public DataProducerProxy(IDataProducer liveProcedure, IProcedureRegistry registry)
    {
        super(liveProcedure, registry);
    }


    @Override
    public void connectLiveProcedure(IProcedureWithState proc)
    {
        this.outputs = new LinkedHashMap<>();
        super.connectLiveProcedure(proc);

        // forward events from all outputs to event bus
        for (IStreamingDataInterface output : ((IDataProducer) proc).getOutputs().values())
            outputs.put(output.getName(), new OutputProxy(output));
    }


    @Override
    public void disconnectLiveProcedure(IProcedureWithState proc)
    {
        Asserts.checkArgument(proc instanceof IDataProducer);        
        super.disconnectLiveProcedure(proc);

        // unregister from output events
        for (OutputProxy output : outputs.values())
            output.disconnect((IDataProducer)proc);
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getOutputs()
    {
        return Collections.unmodifiableMap(outputs);
    }


    @Override
    public AbstractFeature getCurrentFeatureOfInterest()
    {
        IProcedureWithState proc = ref.get();
        if (proc != null)
            return ((IDataProducer) proc).getCurrentFeatureOfInterest();
        else
            return currentFoi;
    }
    
    
    @Override
    protected boolean captureState(IProcedureWithState proc)
    {
        boolean changed = super.captureState(proc);
        
        currentFoi = ((IDataProducer)proc).getCurrentFeatureOfInterest();
        for (OutputProxy output: outputs.values())
            output.captureState();
        
        return changed;
    }
    

    /*
     * class to proxy each data producer output
     */
    class OutputProxy implements IStreamingDataInterface, IEventListener, Serializable
    {
        private static final long serialVersionUID = 835571125683311537L;
        
        transient WeakReference<IStreamingDataInterface> ref;
        transient IEventPublisher eventPublisher;
        
        String name;
        IEventSourceInfo eventSrcInfo;
        DataComponent recordDescription;
        DataEncoding recommmendedEncoding;
        DataBlock latestRecord;
        long latestRecordTime;
        double avgSamplingPeriod = Double.NaN;


        OutputProxy(IStreamingDataInterface liveOutput)
        {
            this.ref = new WeakReference<>(liveOutput);
            this.name = liveOutput.getName();
            this.eventSrcInfo = liveOutput.getEventSourceInfo();
            this.eventPublisher = registry.getParentHub().getEventBus().getPublisher(eventSrcInfo);
            liveOutput.registerListener(this);
        }
        
        
        void captureState()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
            {
                recordDescription = output.getRecordDescription();
                recommmendedEncoding = output.getRecommendedEncoding();
                latestRecordTime = output.getLatestRecordTime();
                latestRecord = output.getLatestRecord();
                avgSamplingPeriod = output.getAverageSamplingPeriod();
            }
        }


        @Override
        public IEventSourceInfo getEventSourceInfo()
        {
            return eventSrcInfo;
        }


        @Override
        public IDataProducer getParentProducer()
        {
            return DataProducerProxy.this;
        }


        @Override
        public String getName()
        {
            return name;
        }


        @Override
        public boolean isEnabled()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
                return output.isEnabled();
            return false;
        }


        @Override
        public DataComponent getRecordDescription()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
                return output.getRecordDescription();
            else
                return recordDescription;
        }


        @Override
        public DataEncoding getRecommendedEncoding()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
                return output.getRecommendedEncoding();
            else
                return recommmendedEncoding;
        }


        @Override
        public DataBlock getLatestRecord()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
                return output.getLatestRecord();
            else
                return latestRecord;
        }


        @Override
        public long getLatestRecordTime()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
                return output.getLatestRecordTime();
            else
                return latestRecordTime;
        }


        @Override
        public double getAverageSamplingPeriod()
        {
            IStreamingDataInterface output = ref.get();
            if (output != null)
                return output.getAverageSamplingPeriod();
            else
                return avgSamplingPeriod;
        }


        @Override
        public void handleEvent(Event e)
        {
            if (e instanceof DataEvent)
            {
                latestRecordTime = e.getTimeStamp();
                latestRecord = ((DataEvent)e).getRecords()[0];
                
                // forward procedure events to bus
                eventPublisher.publish(e);
            }
        }
        
        
        void disconnect(IDataProducer liveProcedure)
        {
            IStreamingDataInterface liveOutput = liveProcedure.getOutputs().get(name);
            liveOutput.unregisterListener(this);
        }


        @Override
        public void registerListener(IEventListener listener)
        {
            throw new UnsupportedOperationException(NO_LISTENER_MSG);
        }


        @Override
        public void unregisterListener(IEventListener listener)
        {
        }
    }

}
