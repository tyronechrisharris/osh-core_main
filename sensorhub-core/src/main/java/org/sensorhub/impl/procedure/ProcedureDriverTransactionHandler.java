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

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.data.ICommandReceiver;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IStreamingControlInterface;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.obs.DataStreamAddedEvent;
import org.sensorhub.api.obs.DataStreamChangedEvent;
import org.sensorhub.api.procedure.IProcedureDriver;
import org.sensorhub.api.procedure.IProcedureGroupDriver;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.procedure.ProcedureWrapper;
import org.sensorhub.api.utils.OshAsserts;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;


public class ProcedureDriverTransactionHandler extends ProcedureTransactionHandler implements IEventListener
{
    protected WeakReference<IProcedureDriver> driverRef; // reference to live procedure
    
    protected Map<String, DataStreamTransactionHandler> dataStreamHandlers = new ConcurrentHashMap<>();
    protected Map<String, ProcedureDriverTransactionHandler> memberHandlers = new ConcurrentHashMap<>();
    
    
    protected ProcedureDriverTransactionHandler(ISensorHub hub, IProcedureObsDatabase db)
    {
        super(hub, db);
    }
        

    @Override    
    protected IEventPublisher getEventPublisher()
    {
        // connect to event bus if needed
        if (eventPublisher == null)
        {
            var proc = driverRef.get();
            if (proc != null)
            {
                var eventSrcInfo = proc.getEventSourceInfo();
                eventPublisher = hub.getEventBus().getPublisher(eventSrcInfo);
            }
        }
        
        return eventPublisher;
    }
    
    
    protected CompletableFuture<Boolean> register(IProcedureDriver proc)
    {
        return CompletableFuture.supplyAsync(() -> {
            try { return doRegister(proc); }
            catch (DataStoreException e) { throw new CompletionException(e); }
        });
    }
    
    
    protected boolean doRegister(IProcedureDriver proc) throws DataStoreException
    {
        return doRegister(0L, proc);
    }
    
    
    protected boolean doRegister(long parentID, IProcedureDriver proc) throws DataStoreException
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);
        OshAsserts.checkValidUID(proc.getUniqueIdentifier());
        DefaultProcedureRegistry.log.info("Registering procedure {}", proc.getUniqueIdentifier());
        
        this.driverRef = new WeakReference<>(Asserts.checkNotNull(proc, IProcedureDriver.class));
        
        // create or update entry in DB
        var isNew = createOrUpdate(parentID, new ProcedureWrapper(proc.getCurrentDescription()));
        enable();
        
        // register to receive driver events
        proc.registerListener(this);
        
        // if data producer, register fois and datastreams
        if (proc instanceof IDataProducer)
        {
            var dataSource = (IDataProducer)proc;
            
            for (var foi: dataSource.getCurrentFeaturesOfInterest().values())
                doRegister(foi);
            
            for (var dataStream: dataSource.getOutputs().values())
                doRegister(dataStream);
        }
        
        // if command sink, register command streams
        if (proc instanceof ICommandReceiver)
        {
            var taskableSource = (ICommandReceiver)proc;
            for (var commanStream: taskableSource.getCommandInputs().values())
                doRegister(commanStream);            
        }
        
        // if group, also register members recursively
        if (proc instanceof IProcedureGroupDriver)
        {
            for (var member: ((IProcedureGroupDriver<?>)proc).getMembers().values())
                doRegisterMember(member);
        }

        if (DefaultProcedureRegistry.log.isInfoEnabled())
        {
            var msg = isNew ?
                String.format("New procedure registered: %s", procUID) :
                String.format("Existing procedure reconnected: %s", procUID);
            
            DefaultProcedureRegistry.log.info("{} ({} FOIs, {} datastreams, {} command inputs, {} members)",
                    msg,
                    proc instanceof IDataProducer ? ((IDataProducer)proc).getCurrentFeaturesOfInterest().size() : 0,
                    proc instanceof IDataProducer ? ((IDataProducer)proc).getOutputs().size() : 0,
                    proc instanceof ICommandReceiver ? ((ICommandReceiver)proc).getCommandInputs().size() : 0,
                    proc instanceof IProcedureGroupDriver ? ((IProcedureGroupDriver<?>)proc).getMembers().size() : 0);
        }
        
        return isNew;
    }
    
    
    protected CompletableFuture<Boolean> registerMember(IProcedureDriver proc)
    {
        return CompletableFuture.supplyAsync(() -> {
            try { return doRegisterMember(proc); }
            catch (DataStoreException e) { throw new CompletionException(e); }
        });
    }
    
    
    protected boolean doRegisterMember(IProcedureDriver proc) throws DataStoreException
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);
        OshAsserts.checkValidUID(proc.getUniqueIdentifier());
        
        var memberHandler = memberHandlers.computeIfAbsent(proc.getUniqueIdentifier(), k -> {
            var newHandler = new ProcedureDriverTransactionHandler(hub, db);
            newHandler.parentGroupUID = procUID;
            return newHandler;
        });
        
        return memberHandler.doRegister(internalID, proc);
    }


    protected CompletableFuture<Void> unregister(IProcedureDriver proc)
    {
        doUnregister(proc);      
        return CompletableFuture.completedFuture(null);
    }
    
    
    protected void doUnregister(IProcedureDriver proc)
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);
        
        proc.unregisterListener(this);
        driverRef.clear();
        DefaultProcedureRegistry.log.debug("Procedure {} disconnected", procUID);
        
        // if data producer, unregister datastreams
        if (proc instanceof IDataProducer)
        {
            var dataSource = (IDataProducer)proc;
            for (var dataStream: dataSource.getOutputs().values())
                doUnregister(dataStream);
        }
        
        // if command sink, unregister command streams
        if (proc instanceof ICommandReceiver)
        {
            var taskableSource = (ICommandReceiver)proc;
            for (var commanStream: taskableSource.getCommandInputs().values())
                doUnregister(commanStream);            
        }
        
        // if group, also unregister members recursively
        if (proc instanceof IProcedureGroupDriver)
        {
            for (var member: ((IProcedureGroupDriver<?>)proc).getMembers().values())
                doUnregister(member);
        }
        
        disable();
    }


    protected CompletableFuture<Boolean> register(IStreamingDataInterface output)
    {
        return CompletableFuture.supplyAsync(() -> {
            try { return doRegister(output); }
            catch (DataStoreException e) { throw new CompletionException(e); }
        });
    }
    
    
    protected boolean doRegister(IStreamingDataInterface output) throws DataStoreException
    {
        Asserts.checkNotNull(output, IStreamingDataInterface.class);
        
        // get or create datastream handler
        var dsHandler = dataStreamHandlers.computeIfAbsent(output.getName(), k -> {
            var newHandler = new DataStreamTransactionHandler(hub, db.getObservationStore(), foiIdMap);
            newHandler.parentGroupUID = parentGroupUID;
            return newHandler;
        });
        
        // add or update metadata
        var isNew = dsHandler.createOrUpdate(
            new ProcedureId(internalID, procUID),
            output.getName(),
            output.getRecordDescription(),
            output.getRecommendedEncoding());
        
        // notify if new
        if (isNew)
        {
            getEventPublisher().publish(
                new DataStreamAddedEvent(procUID, output.getName()));
        }
        
        // enable and start forwarding events
        enableDataStream(output.getName());
        output.registerListener(dsHandler);
        
        return isNew;
    }


    protected CompletableFuture<Void> unregister(IStreamingDataInterface output)
    {
        doUnregister(output);
        return CompletableFuture.completedFuture(null);
    }
    
    
    protected void doUnregister(IStreamingDataInterface output)
    {
        Asserts.checkNotNull(output, IStreamingDataInterface.class);
        
        var dsHandler = dataStreamHandlers.remove(output.getName());
        if (dsHandler != null)
        {
            output.unregisterListener(dsHandler);
            disableDataStream(output.getName());
        }
    }
    
    
    @Override
    public boolean deleteDataStream(String outputName)
    {
        var driver = driverRef.get();
        if (driver != null)
        {
            var output = ((IDataProducer)driver).getOutputs().get(outputName);
            if (output != null)
                doUnregister(output);
        }        
        
        return super.deleteDataStream(outputName);
    }
    
    
    @Override
    public void deleteMember(String uid)
    {
        checkInitialized();
        
        var memberHandler = memberHandlers.remove(uid);
        if (memberHandler != null)
        {
            memberHandler.parentGroupUID = procUID;
            memberHandler.delete();
        }
    }


    protected CompletableFuture<Boolean> register(IStreamingControlInterface controlStream)
    {
        Asserts.checkNotNull(controlStream, IStreamingControlInterface.class);
        
        return CompletableFuture.supplyAsync(() -> {
            return doRegister(controlStream);
        });
    }
    
    
    protected boolean doRegister(IStreamingControlInterface commandStream)
    {
        DefaultProcedureRegistry.log.warn("Command streams register not implemented yet");
        return true;
    }


    protected CompletableFuture<Void> unregister(IStreamingControlInterface commandStream)
    {
        Asserts.checkNotNull(commandStream, IStreamingControlInterface.class);
        doUnregister(commandStream);
        return CompletableFuture.completedFuture(null);
    }
    
    
    protected void doUnregister(IStreamingControlInterface proc)
    {
        DefaultProcedureRegistry.log.warn("Command streams unregister not implemented yet");
    }


    protected CompletableFuture<Boolean> register(IGeoFeature foi)
    {
        Asserts.checkNotNull(foi, IGeoFeature.class);
        OshAsserts.checkValidUID(foi.getUniqueIdentifier());
        
        return CompletableFuture.supplyAsync(() -> {
            try { return doRegister(foi); }
            catch (DataStoreException e) { throw new CompletionException(e); }
        });
    }
    
    
    protected boolean doRegister(IGeoFeature foi) throws DataStoreException
    {
        return addOrUpdateFoi(foi);
    }
    
    
    @Override
    public ProcedureDriverTransactionHandler addOrUpdateMember(IProcedureWithDesc proc) throws DataStoreException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void handleEvent(Event e)
    {
        if (e instanceof ProcedureEvent)
        {
            // register item if needed
            var proc = driverRef.get();
            if (proc != null && proc.isEnabled())
            {
                if (proc instanceof IDataProducer)
                {
                    var outputs = ((IDataProducer)proc).getOutputs();
                    
                    if (e instanceof DataStreamAddedEvent || e instanceof DataStreamChangedEvent)
                        register(outputs.get(((DataStreamAddedEvent)e).getOutputName()));
                    else if (e instanceof DataStreamAddedEvent)
                        register(outputs.get(((DataStreamAddedEvent)e).getOutputName()));
                    // TODO handle all event types and redirect to register methods when appropriate
                }
            }
        }
        
        else if (e instanceof FoiEvent)
        {
            if (((FoiEvent) e).getFoi() != null)
                register(((FoiEvent) e).getFoi());
        }
        
    }

}
