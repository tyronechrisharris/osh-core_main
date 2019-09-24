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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IFeatureFilter;
import org.sensorhub.api.datastore.IProcedureStore;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.procedure.IProcedureWithState;
import org.sensorhub.api.procedure.IProcedureGroup;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.procedure.IProcedureShadowStore;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.impl.datastore.InMemoryProcedureStore;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.utils.MsgUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import org.vast.util.Bbox;


/**
 * <p>
 * implementation of procedure registry backed by a configurable
 * datastore.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 7, 2019
 */
public class DefaultProcedureRegistry implements IProcedureRegistry
{
    private static final Logger log = LoggerFactory.getLogger(DefaultProcedureRegistry.class);
    
    ISensorHub hub;
    IProcedureShadowStore dataStore;
    IEventPublisher eventHandler;
    ReadWriteLock lock = new ReentrantReadWriteLock();
    Map<String, ProcedureProxy> procedureProxies = new TreeMap<>();
    
    
    static class InMemoryShadowStore extends InMemoryProcedureStore<IProcedureWithState> implements IProcedureShadowStore
    {        
    }
    
    
    public DefaultProcedureRegistry(ISensorHub hub)
    {
        this(hub, new InMemoryShadowStore());
    }
    
    
    public DefaultProcedureRegistry(ISensorHub hub, IProcedureShadowStore dataStore)
    {
        Asserts.checkNotNull(hub, ISensorHub.class);
        Asserts.checkNotNull(dataStore, IProcedureStore.class);
        
        this.hub = hub;
        this.dataStore = dataStore;
        this.eventHandler = hub.getEventBus().getPublisher(IProcedureRegistry.EVENT_SOURCE_ID);
    }
    
    
    @Override
    public FeatureKey register(IProcedureWithState proc)
    {
        String groupUID = null;
        if (proc.getParentGroup() != null)
            groupUID = proc.getParentGroup().getUniqueIdentifier();
        return register(proc, groupUID);
    }
        
        
    protected FeatureKey register(IProcedureWithState proc, String groupUID)
    {
        Asserts.checkNotNull(proc, IProcedureWithState.class);
        
        lock.writeLock().lock();
        String uid = proc.getUniqueIdentifier();
        log.debug("Registering procedure {}", uid);
        
        try
        {
            ProcedureProxy proxy = getProxy(proc);
            procedureProxies.put(uid, proxy);
            
            // save in data store if needed
            FeatureKey key = proxy.updateInDatastore(true);
            
            // publish procedure added event
            eventHandler.publish(new ProcedureAddedEvent(System.currentTimeMillis(), uid, groupUID));
            
            // if group, also register members recursively
            if (proc instanceof IProcedureGroup)
            {
                for (IProcedureWithState member: ((IProcedureGroup<?>)proc).getMembers().values())
                    register(member, uid);
            }
            
            return key;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    
    
    protected ProcedureProxy getProxy(IProcedureWithState proc)
    {
        if (proc instanceof ProcedureProxy)
            return (ProcedureProxy)proc;
        else if (proc instanceof IDataProducer)
            return new DataProducerProxy((IDataProducer)proc, this);
        
        return new ProcedureProxy(proc, this);
    }
    
    
    @Override
    public void unregister(IProcedureWithState proc)
    {
        String groupUID = null;
        if (proc.getParentGroup() != null)
            groupUID = proc.getParentGroup().getUniqueIdentifier();
        unregister(proc, groupUID);
    }
    
    
    protected void unregister(IProcedureWithState proc, String groupUID)
    {
        Asserts.checkNotNull(proc, IProcedureWithState.class);
        
        lock.writeLock().lock();
        String uid = proc.getUniqueIdentifier();
        log.debug("Unregistering procedure {}", uid);
        
        try
        {
            // if entity group, unregister members recursively
            if (proc instanceof IProcedureGroup)
            {
                for (IProcedureWithState member: ((IProcedureGroup<?>)proc).getMembers().values())
                    unregister(member);
            }
            
            // remove from proxy list
            ProcedureProxy proxy = procedureProxies.remove(uid);
            proxy.disconnectLiveProcedure(proc);
            
            // remove from data store
            dataStore.remove(uid);
            
            // publish procedure removed event
            eventHandler.publish(new ProcedureRemovedEvent(System.currentTimeMillis(), uid, groupUID));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }
    

    @Override
    public IProcedureWithState get(String uid)
    {
        lock.readLock().lock();
        
        try
        {
            // look by local ID in the module registry for backward compatibility
            ModuleRegistry moduleRegistry = hub.getModuleRegistry();
            if (moduleRegistry.isModuleLoaded(uid))
            {
                IModule<?> module = moduleRegistry.getModuleById(uid);
                if (!(module instanceof IProcedureWithState))
                    throw new IllegalArgumentException("Module " + MsgUtils.moduleString(module) + " is not a procedure");
                return get(((IProcedureWithState)module).getUniqueIdentifier());
            }            
            else
            {
                IProcedureWithState proxy = procedureProxies.computeIfAbsent(uid, k -> {
                    ProcedureProxy proc = (ProcedureProxy)dataStore.get(k);
                    if (proc == null)
                        throw new IllegalArgumentException("Cannot find procedure " + uid);
                    proc.setProcedureRegistry(this);
                    return proc;
                });
                
                return proxy;
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
    public ISensorHub getParentHub()
    {
        return hub;
    }
    
    
    /*
     * Methods delegated to underlying data store
     */

    @Override
    public FeatureKey add(IProcedureWithState feature)
    {
        return dataStore.add(feature);
    }
    
    
    @Override
    public FeatureKey addVersion(IProcedureWithState feature)
    {
        return dataStore.addVersion(feature);
    }


    @Override
    public void clear()
    {
        dataStore.clear();
    }


    @Override
    public IProcedureWithState compute(FeatureKey key, BiFunction<? super FeatureKey, ? super IProcedureWithState, ? extends IProcedureWithState> remappingFunction)
    {
        return (IProcedureWithState)dataStore.compute(key, remappingFunction);
    }


    @Override
    public IProcedureWithState computeIfAbsent(FeatureKey arg0, Function<? super FeatureKey, ? extends IProcedureWithState> arg1)
    {
        return dataStore.computeIfAbsent(arg0, arg1);
    }


    @Override
    public IProcedureWithState computeIfPresent(FeatureKey arg0, BiFunction<? super FeatureKey, ? super IProcedureWithState, ? extends IProcedureWithState> arg1)
    {
        return dataStore.computeIfPresent(arg0, arg1);
    }


    @Override
    public boolean contains(String uid)
    {
        return dataStore.contains(uid);
    }


    @Override
    public boolean containsKey(Object key)
    {
        return dataStore.containsKey(key);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return dataStore.containsValue(val);
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        return dataStore.getTimeZone();
    }


    @Override
    public long getNumRecords()
    {
        return dataStore.getNumRecords();
    }


    @Override
    public long countMatchingEntries(IFeatureFilter query)
    {
        return dataStore.countMatchingEntries(query);
    }


    @Override
    public Set<Entry<FeatureKey, IProcedureWithState>> entrySet()
    {
        return dataStore.entrySet();
    }


    @Override
    public void forEach(BiConsumer<? super FeatureKey, ? super IProcedureWithState> func)
    {
        dataStore.forEach(func);
    }


    @Override
    public IProcedureWithState get(Object key)
    {
        return dataStore.get(key);
    }


    @Override
    public IProcedureWithState getOrDefault(Object key, IProcedureWithState defaultValue)
    {
        return dataStore.getOrDefault(key, defaultValue);
    }


    @Override
    public String getDatastoreName()
    {
        return dataStore.getDatastoreName();
    }


    @Override
    public long getNumFeatures()
    {
        return dataStore.getNumFeatures();
    }
    
    
    @Override
    public FeatureId getFeatureID(FeatureKey key)
    {
        return dataStore.getFeatureID(key);
    }


    @Override
    public Stream<FeatureId> getAllFeatureIDs()
    {
        return dataStore.getAllFeatureIDs();
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        return dataStore.getFeaturesBbox();
    }


    @Override
    public boolean isEmpty()
    {
        return dataStore.isEmpty();
    }


    @Override
    public int size()
    {
        return dataStore.size();
    }


    @Override
    public Collection<IProcedureWithState> values()
    {
        return dataStore.values();
    }


    @Override
    public boolean isReadSupported()
    {
        return dataStore.isReadSupported();
    }


    @Override
    public boolean isWriteSupported()
    {
        return dataStore.isWriteSupported();
    }


    @Override
    public Set<FeatureKey> keySet()
    {
        return dataStore.keySet();
    }


    @Override
    public IProcedureWithState merge(FeatureKey key, IProcedureWithState value, BiFunction<? super IProcedureWithState, ? super IProcedureWithState, ? extends IProcedureWithState> remappingFunction)
    {
        return dataStore.merge(key, value, remappingFunction);
    }


    @Override
    public IProcedureWithState put(FeatureKey arg0, IProcedureWithState arg1)
    {
        return dataStore.put(arg0, arg1);
    }


    @Override
    public void putAll(Map<? extends FeatureKey, ? extends IProcedureWithState> arg0)
    {
        dataStore.putAll(arg0);
    }


    @Override
    public IProcedureWithState putIfAbsent(FeatureKey key, IProcedureWithState value)
    {
        return dataStore.putIfAbsent(key, value);
    }


    @Override
    public boolean remove(Object key, Object value)
    {
        return dataStore.remove(key, value);
    }


    @Override
    public IProcedureWithState remove(Object key)
    {
        IProcedureWithState proc = dataStore.remove(key);
        this.procedureProxies.remove(proc.getUniqueIdentifier());
        return proc;
    }


    @Override
    public boolean remove(String uid)
    {
        return dataStore.remove(uid);
    }


    @Override
    public Stream<FeatureKey> removeEntries(IFeatureFilter query)
    {
        return dataStore.removeEntries(query);
    }


    @Override
    public boolean replace(FeatureKey key, IProcedureWithState oldValue, IProcedureWithState newValue)
    {
        return dataStore.replace(key, oldValue, newValue);
    }


    @Override
    public IProcedureWithState replace(FeatureKey key, IProcedureWithState value)
    {
        return dataStore.replace(key, value);
    }


    @Override
    public void replaceAll(BiFunction<? super FeatureKey, ? super IProcedureWithState, ? extends IProcedureWithState> arg0)
    {
        dataStore.replaceAll(arg0);
    }


    @Override
    public Stream<IProcedureWithState> select(IFeatureFilter query)
    {
        return dataStore.select(query);
    }


    @Override
    public Stream<FeatureKey> selectKeys(IFeatureFilter query)
    {
        return dataStore.selectKeys(query);
    }


    @Override
    public Stream<Entry<FeatureKey, IProcedureWithState>> selectEntries(IFeatureFilter query)
    {
        return dataStore.selectEntries(query);
    }


    @Override
    public void commit()
    {
        dataStore.commit();
    }


    @Override
    public void backup(OutputStream is) throws IOException
    {
        dataStore.backup(is);
    }


    @Override
    public void restore(InputStream os) throws IOException
    {
        dataStore.restore(os);
    }

}
