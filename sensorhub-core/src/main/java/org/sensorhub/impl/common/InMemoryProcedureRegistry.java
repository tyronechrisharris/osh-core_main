/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.IProcedure;
import org.sensorhub.api.common.IProcedureGroup;
import org.sensorhub.api.common.IProcedureRegistry;
import org.sensorhub.api.common.ProcedureAddedEvent;
import org.sensorhub.api.common.ProcedureEvent;
import org.sensorhub.api.common.ProcedureRemovedEvent;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.module.IModule;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.persistence.FilteredIterator;
import org.sensorhub.utils.MsgUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;


/**
 * <p>
 * Default implementation maintaining a map of all registered procedures in
 * memory and handling registration of the procedures with the event bus.
 * </p>
 *
 * @author Alex Robin
 * @since Jun 11, 2017
 */
public class InMemoryProcedureRegistry implements IProcedureRegistry
{
    private static final Logger log = LoggerFactory.getLogger(InMemoryProcedureRegistry.class);
    
    ISensorHub hub;
    IEventPublisher eventHandler;
    ReadWriteLock lock = new ReentrantReadWriteLock();
    Map<String, WeakReference<IProcedure>> rootProcedures = new TreeMap<>();
    Map<String, WeakReference<IProcedure>> allProcedures = new TreeMap<>();
    
    // we need to keep a strong reference to lambdas used as listeners
    // or they get garbage collected!!
    Map<String, IEventListener> eventListeners = new HashMap<>();
    
    
    public InMemoryProcedureRegistry(ISensorHub hub)
    {
        this.hub = hub;
        this.eventHandler = hub.getEventBus().getPublisher(IProcedureRegistry.EVENT_SOURCE_ID);
    }
    
    
    @Override
    public void register(IProcedure proc)
    {
        Asserts.checkNotNull(proc, IProcedure.class);
        
        lock.writeLock().lock();
        log.debug("Registering procedure {}", proc.getUniqueIdentifier());
        
        try
        {
            WeakReference<IProcedure> ref = new WeakReference<>(proc);
            
            // always add to list of all procedures
            allProcedures.put(proc.getUniqueIdentifier(), ref);
            
            // only add to root list if procedure has no parent
            if (proc.getParentGroup() == null)
                rootProcedures.put(proc.getUniqueIdentifier(), ref);
            
            // register procedure on event bus and forward its events to it
            registerWithEventBus(proc);
            
            // send general procedure added event
            eventHandler.publish(new ProcedureAddedEvent(System.currentTimeMillis(), proc.getUniqueIdentifier(), null));
            
            // if group, also register members recursively
            if (proc instanceof IProcedureGroup)
            {
                for (IProcedure member: ((IProcedureGroup<?>)proc).getMembers().values())
                    register(member);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    
    protected void registerWithEventBus(IProcedure proc)
    {
        // register the procedure itself
        IEventSourceInfo eventSrcInfo = proc.getEventSourceInfo();
        final IEventPublisher eventPublisher = hub.getEventBus().getPublisher(eventSrcInfo);
        
        eventListeners.computeIfAbsent(eventSrcInfo.getSourceID(), k -> {
            IEventListener listener = e -> {
                if (e instanceof ProcedureEvent)
                    eventPublisher.publish(e);
            };
            proc.registerListener(listener);
            return listener;
        });
        
        
        // if data producer, register all outputs
        if (proc instanceof IDataProducer)
        {
            for (IStreamingDataInterface output: ((IDataProducer) proc).getOutputs().values())
            {
                eventSrcInfo = output.getEventSourceInfo();
                final IEventPublisher outputPublisher = hub.getEventBus().getPublisher(eventSrcInfo);
                
                eventListeners.computeIfAbsent(eventSrcInfo.getSourceID(), k -> {
                    IEventListener listener = outputPublisher::publish;
                    output.registerListener(listener);
                    return listener;
                });
            }
        }
    }


    @Override
    public void unregister(String uid)
    {
        Asserts.checkNotNull(uid, "uid");
        
        lock.writeLock().lock();
        log.debug("Registering procedure {}", uid);
        
        try
        {
            // always remove from list of all procedures
            IProcedure proc;
            WeakReference<IProcedure> ref = allProcedures.remove(uid);
            if (ref == null || (proc = ref.get()) == null)
                throw new IllegalArgumentException("Cannot find procedure " + uid);
            
            // unregister from event bus
            unregisterFromEventBus(proc);
            
            // if entity group, unregister members recursively
            rootProcedures.remove(uid);            
            if (proc instanceof IProcedureGroup)
            {
                for (IProcedure member: ((IProcedureGroup<?>)proc).getMembers().values())
                    unregister(member.getUniqueIdentifier());
            }            

            eventHandler.publish(new ProcedureRemovedEvent(System.currentTimeMillis(), proc.getUniqueIdentifier(), null));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    
    protected void unregisterFromEventBus(IProcedure proc)
    {
        // register the procedure itself
        IEventSourceInfo eventSrcInfo = proc.getEventSourceInfo();
        IEventListener listener = eventListeners.remove(eventSrcInfo.getSourceID());
        if (listener != null)
            proc.unregisterListener(listener);
        
        // if data producer, register all outputs
        if (proc instanceof IDataProducer)
        {
            for (IStreamingDataInterface output: ((IDataProducer) proc).getOutputs().values())
            {
                eventSrcInfo = output.getEventSourceInfo();
                listener = eventListeners.remove(eventSrcInfo.getSourceID());
                if (listener != null)
                    output.unregisterListener(listener);
            }
        }
    }


    @Override
    public <T extends IProcedure> T get(String uid)
    {
        WeakReference<T> ref = getRef(uid);
        if (ref == null)
            return null;
        return ref.get();
    }


    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends IProcedure> WeakReference<T> getRef(String uid)
    {
        lock.readLock().lock();
        
        try
        {
            // look by local ID in the module registry for backward compatibility
            ModuleRegistry moduleRegistry = hub.getModuleRegistry();
            if (moduleRegistry.isModuleLoaded(uid))
            {
                WeakReference<IModule<?>> moduleRef = moduleRegistry.getModuleRef(uid);
                IModule<?> module = moduleRef.get();
                if (!(module instanceof IProcedure))
                    throw new IllegalArgumentException("Module " + MsgUtils.moduleString(module) + " is not a procedure");
                return (WeakReference)moduleRef;
            }            
            else
            {
                WeakReference<IProcedure> ref = allProcedures.get(uid);
                if (ref == null)
                    throw new IllegalArgumentException("Cannot find procedure " + uid);
                return (WeakReference<T>)ref;
            }
        }
        catch (SensorHubException e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
        finally
        {
            lock.readLock().unlock();
        }
    }


    @Override
    public boolean contains(String uid)
    {
        return allProcedures.containsKey(uid);
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <T extends IProcedure> Iterable<T> list(Class<T> procedureType)
    {
        // return a filtered iterable
        return () -> (Iterator) new FilteredIterator<WeakReference<IProcedure>>(rootProcedures.values().iterator())
        {
            @Override
            protected boolean accept(WeakReference<IProcedure> ref)
            {
                IProcedure proc;
                if (ref == null || (proc = ref.get()) == null)
                    return false;
                return procedureType.isAssignableFrom(proc.getClass());
            }
        };
    }
}
