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

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.sensorhub.api.common.CommandStatus;
import org.sensorhub.api.common.CommandStatus.StatusCode;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.ICommandReceiver;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IStreamingControlInterface;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.procedure.IProcedureDriver;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.sensor.ISensorDriver;
import org.sensorhub.api.sensor.SensorException;
import org.sensorhub.impl.procedure.DefaultProcedureRegistry;
import org.sensorhub.impl.procedure.ProcedureShadow;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Wrapper class reflecting the latest state of the attached sensor.<br/>
 * @see {@link ProcedureShadow}
 * </p>
 *
 * @author Alex Robin
 * @date Sep 6, 2019
 */
public class SensorShadow extends ProcedureShadow implements ISensorDriver
{
    private static final long serialVersionUID = -6315464994380209210L;
    
    protected Map<String, OutputProxy> obsOutputs = new LinkedHashMap<>();
    protected Map<String, OutputProxy> statusOutputs = new LinkedHashMap<>();
    protected Map<String, ControlProxy> controlInputs = new LinkedHashMap<>();
    protected IGeoFeature currentFoi;


    public SensorShadow(ProcedureId procId, IDataProducer liveProcedure, DefaultProcedureRegistry registry)
    {
        super(procId, liveProcedure, registry);
    }


    @Override
    public void connectLiveProcedure(IProcedureDriver proc)
    {
        Asserts.checkArgument(proc instanceof IDataProducer);
        
        if (proc instanceof IDataProducer)
        {
            if (proc instanceof ISensorDriver)
            {
                for (IStreamingDataInterface output : ((ISensorDriver) proc).getObservationOutputs().values())
                    obsOutputs.put(output.getName(), new OutputProxy(output));
                
                for (IStreamingDataInterface output : ((ISensorDriver) proc).getStatusOutputs().values())
                    statusOutputs.put(output.getName(), new OutputProxy(output));
            }
            else
            {
                for (IStreamingDataInterface output : ((IDataProducer) proc).getOutputs().values())
                    obsOutputs.put(output.getName(), new OutputProxy(output));
            }
        }
        
        if (proc instanceof ICommandReceiver)
        {
            for (IStreamingControlInterface input : ((ICommandReceiver) proc).getCommandInputs().values())
                controlInputs.put(input.getName(), new ControlProxy(input));
        }
        
        super.connectLiveProcedure(proc);          
    }


    @Override
    public void disconnectLiveProcedure(IProcedureDriver proc)
    {
        Asserts.checkArgument(proc instanceof IDataProducer);        
        super.disconnectLiveProcedure(proc);

        // unregister from output events
        for (OutputProxy output : obsOutputs.values())
            output.disconnect((IDataProducer)proc);
        for (OutputProxy output : statusOutputs.values())
            output.disconnect((IDataProducer)proc);
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getOutputs()
    {
        Map<String, IStreamingDataInterface> allOutputs = new LinkedHashMap<>();  
        allOutputs.putAll(obsOutputs);
        allOutputs.putAll(statusOutputs);
        return Collections.unmodifiableMap(allOutputs);
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getStatusOutputs()
    {
        return Collections.unmodifiableMap(statusOutputs);
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getObservationOutputs()
    {
        return Collections.unmodifiableMap(obsOutputs);
    }


    @Override
    public Map<String, ? extends IStreamingControlInterface> getCommandInputs()
    {
        return Collections.unmodifiableMap(controlInputs);
    }


    @Override
    public IGeoFeature getCurrentFeatureOfInterest()
    {
        IProcedureDriver proc = ref.get();
        if (proc != null)
            return ((IDataProducer) proc).getCurrentFeatureOfInterest();
        else
            return currentFoi;
    }


    @Override
    public boolean isConnected()
    {
        IProcedureDriver proc = ref.get();
        if (proc == null)
            return false;
        
        if (proc instanceof ISensorDriver)
            return ((ISensorDriver)proc).isConnected();
        
        return false;
    }
    
    
    @Override
    protected boolean captureState(IProcedureDriver proc)
    {
        boolean changed = super.captureState(proc);
        
        currentFoi = ((IDataProducer)proc).getCurrentFeatureOfInterest();
        
        for (OutputProxy output: obsOutputs.values())
            output.captureState();
        for (OutputProxy output: statusOutputs.values())
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
        IEventSourceInfo eventSrcInfo;
        DataComponent recordDescription;
        DataEncoding recommmendedEncoding;
        DataBlock latestRecord;
        long latestRecordTime = Long.MIN_VALUE;
        double avgSamplingPeriod = Double.NaN;

        OutputProxy()
        {            
        }
        
        OutputProxy(IStreamingDataInterface liveOutput)
        {
            this.ref = new WeakReference<>(liveOutput);
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
            return SensorShadow.this;
        }

        @Override
        public String getName()
        {
            return getRecordDescription().getName();
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
            IStreamingDataInterface liveOutput = liveProcedure.getOutputs().get(getName());
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
    
    
    /*
     * class to proxy each control input
     */
    class ControlProxy implements IStreamingControlInterface, Serializable
    {
        private static final long serialVersionUID = 6572968037145053862L;

        transient WeakReference<IStreamingControlInterface> ref;
        DataComponent commandDescription;
        DataEncoding recommmendedEncoding;
        
        ControlProxy(IStreamingControlInterface liveInput)
        {
            this.ref = new WeakReference<>(liveInput);
        }
        
        @Override
        public ICommandReceiver getParentProducer()
        {
            return SensorShadow.this;
        }
        
        @Override
        public String getName()
        {
            return getCommandDescription().getName();
        }
        
        @Override
        public boolean isEnabled()
        {
            IStreamingControlInterface input = ref.get();
            if (input != null)
                return input.isEnabled();
            return false;
        }
        
        @Override
        public DataComponent getCommandDescription()
        {
            IStreamingControlInterface input = ref.get();
            if (input != null)
                return input.getCommandDescription();
            else
                return commandDescription;
        }
        
        @Override
        public CommandStatus execCommand(DataBlock command) throws SensorException
        {
            IStreamingControlInterface input = ref.get();
            if (input != null)
                return input.execCommand(command);
            return new CommandStatus("NOID", StatusCode.REJECTED);
        }
        
        @Override
        public CommandStatus execCommandGroup(List<DataBlock> commands) throws SensorException
        {
            IStreamingControlInterface input = ref.get();
            if (input != null)
                return input.execCommandGroup(commands);
            return new CommandStatus("NOID", StatusCode.REJECTED);
        }
    }

}
