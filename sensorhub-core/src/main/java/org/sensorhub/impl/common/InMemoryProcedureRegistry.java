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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.IProcedure;
import org.sensorhub.api.common.IProcedureGroup;
import org.sensorhub.api.common.IProcedureRegistry;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.IEventPublisher;
import org.sensorhub.api.module.IModule;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.persistence.FilteredIterator;
import org.sensorhub.utils.MsgUtils;


/**
 * <p>
 * Default implementation of {@link IProcedureRegistry} maintaining a map of
 * all registered procedures in memory
 * </p>
 *
 * @author Alex Robin
 * @since Jun 11, 2017
 */
public class InMemoryProcedureRegistry implements IProcedureRegistry
{
    ReadWriteLock lock = new ReentrantReadWriteLock();
    ModuleRegistry moduleRegistry;
    Map<String, WeakReference<IProcedure>> rootProcedures = new TreeMap<>();
    Map<String, WeakReference<IProcedure>> allProcedures = new TreeMap<>();
    IEventPublisher eventHandler;
    
    
    public InMemoryProcedureRegistry(ISensorHub hub)
    {
        this.moduleRegistry = hub.getModuleRegistry();
        this.eventHandler = hub.getEventBus().getPublisher(SensorHub.PROCEDURE_REGISTRY_ID);
    }
    
    
    @Override
    public void register(IProcedure proc)
    {
        lock.writeLock().lock();
        
        try
        {
            WeakReference<IProcedure> ref = new WeakReference<>(proc);
            
            // always add to list of all procedures
            allProcedures.put(proc.getUniqueIdentifier(), ref);
            
            // only add to root list if procedure has no parent
            if (proc.getParentGroup() == null)
                rootProcedures.put(proc.getUniqueIdentifier(), ref);
            
            if (proc instanceof IProcedureGroup)
            {
                // also register members recursively
                for (IProcedure member: ((IProcedureGroup<?>)proc).getMembers().values())
                    register(member);
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }


    @Override
    public void unregister(String uid)
    {
        lock.writeLock().lock();
        
        try
        {
            // always remove from list of all procedures
            IProcedure proc;
            WeakReference<IProcedure> ref = allProcedures.remove(uid);
            if (ref == null || (proc = ref.get()) == null)
                throw new IllegalArgumentException("Cannot find procedure " + uid);
            
            // if entity group, unregister members recursively
            rootProcedures.remove(uid);            
            if (proc instanceof IProcedureGroup)
            {
                for (IProcedure member: ((IProcedureGroup<?>)proc).getMembers().values())
                    unregister(member.getUniqueIdentifier());
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }


    @Override
    public IProcedure get(String uid)
    {
        lock.readLock().lock();
        
        try
        {
            // look by local ID in the module registry for backward compatibility
            if (moduleRegistry.isModuleLoaded(uid))
            {
                IModule<?> module = moduleRegistry.getLoadedModuleById(uid);
                if (!(module instanceof IProcedure))
                    throw new IllegalArgumentException("Module " + MsgUtils.moduleString(module) + " is not a procedure");
                return (IProcedure)module;
            }            
            else
            {
                IProcedure proc;
                WeakReference<IProcedure> ref = allProcedures.remove(uid);
                if (ref == null || (proc = ref.get()) == null)
                    throw new IllegalArgumentException("Cannot find procedure " + uid); 
                return proc;
            }
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


    @Override
    @SuppressWarnings("unchecked")
    public <T extends IProcedure> Iterable<T> list(Class<T> procedureType)
    {
        // return a filtered iterable
        return () -> (Iterator<T>) new FilteredIterator<WeakReference<IProcedure>>(rootProcedures.values().iterator())
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


    @Override
    public void registerListener(IEventListener listener)
    {
        eventHandler.registerListener(listener);        
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        eventHandler.unregisterListener(listener);        
    }
}
