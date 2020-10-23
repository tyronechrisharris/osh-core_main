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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.datastore.DatabaseConfig;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.procedure.IProcedureDriver;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.procedure.IProcedureGroupDriver;
import org.sensorhub.api.procedure.IProcedureObsDatabase;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.procedure.IProcedureStateDatabase;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.impl.datastore.obs.GenericObsStreamDataStore;
import org.sensorhub.impl.datastore.obs.StreamDataStoreConfig;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.sensor.SensorShadow;
import org.sensorhub.utils.MsgUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of procedure registry backed by a configurable datastore.
 * </p><p>
 * An in-memory map of all procedure proxies registered since the hub startup
 * is maintained to handle events (e.g. to keep procedure state up to date
 * and forward events to event bus).
 * </p>
 *
 * @author Alex Robin
 * @date Sep 7, 2019
 */
public class DefaultProcedureRegistry implements IProcedureRegistry
{
    static final Logger log = LoggerFactory.getLogger(DefaultProcedureRegistry.class);

    ISensorHub hub;
    IEventPublisher eventPublisher;
    GenericObsStreamDataStore procStateDb;
    IProcedureObsDatabase federatedDb;
    ReadWriteLock lock = new ReentrantReadWriteLock();

    // map to cache and pin proxies in memory so they don't get GC
    Map<String, ProcedureShadow> procedureShadows = new TreeMap<>();


    public DefaultProcedureRegistry(ISensorHub hub, DatabaseConfig stateDbConfig)
    {
        this.hub = Asserts.checkNotNull(hub, ISensorHub.class);
        this.eventPublisher = hub.getEventBus().getPublisher(IProcedureRegistry.EVENT_SOURCE_ID);
        initDatabase(stateDbConfig);
    }


    void initDatabase(DatabaseConfig stateDbConfig)
    {
        this.federatedDb = hub.getDatabaseRegistry().getFederatedObsDatabase();
        
        try
        {
            StreamDataStoreConfig dbListenerConfig = new StreamDataStoreConfig();
            dbListenerConfig.dbConfig = stateDbConfig;

            procStateDb = new GenericObsStreamDataStore();
            procStateDb.setParentHub(hub);
            procStateDb.init(dbListenerConfig);
            procStateDb.start();
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Error initializing procedure state database", e);
        }        
    }


    @Override
    public ProcedureId register(IProcedureDriver proc)
    {
        return register(proc, true);
    }


    protected ProcedureId register(IProcedureDriver proc, boolean sendEvent)
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);

        lock.writeLock().lock();
        String uid = proc.getUniqueIdentifier();
        log.debug("Registering procedure {}", uid);

        try
        {
            // save in data store if needed
            FeatureKey key = addToDataStore(proc);
            ProcedureId procId = new ProcedureId(key.getInternalID(), uid);

            // create shadow or simply reconnect to it
            ProcedureShadow shadow = procedureShadows.get(uid);
            boolean isNew = (shadow == null);
            if (isNew)
            {
                shadow = createProxy(procId, proc);
                shadow.connectLiveProcedure(proc);
                procedureShadows.put(uid, shadow);
            }
            else
            {
                IProcedureDriver liveProc = shadow.ref.get();
                if (liveProc != null && liveProc != proc)
                    throw new IllegalArgumentException("A procedure with ID " + uid + " is already registered");
                shadow.connectLiveProcedure(proc);
            }

            // if group, also register members recursively
            if (proc instanceof IProcedureGroupDriver)
            {
                for (IProcedureDriver member: ((IProcedureGroupDriver<?>)proc).getMembers().values())
                    register(member, false);
            }

            // publish procedure added event
            if (sendEvent && isNew)
                eventPublisher.publish(new ProcedureAddedEvent(System.currentTimeMillis(), procId, shadow.getParentGroupID()));

            return procId;
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }


    protected FeatureKey addToDataStore(IProcedureDriver proc)
    {
        // check if procedure data is handled by a dedicated DB
        FeatureKey existingKey = federatedDb.getProcedureStore().getCurrentVersionKey(proc.getUniqueIdentifier());
        if (existingKey != null)
        {
            long databaseID = hub.getDatabaseRegistry().getDatabaseID(existingKey.getInternalID());
            if (databaseID != procStateDb.getDatabaseID())
                return existingKey;
        }
        
        // if procedure not in DB yet, add to procedure state DB
        // TODO only add version if description has changed
        FeatureKey newKey = procStateDb.getProcedureStore().addVersion(proc.getCurrentDescription());
        if (proc instanceof IDataProducer)
            procStateDb.getConfiguration().procedureUIDs.add(proc.getUniqueIdentifier());

        return newKey;
    }


    protected ProcedureShadow createProxy(ProcedureId procId, IProcedureDriver proc)
    {
        ProcedureShadow shadow;
        
        if (proc instanceof ProcedureShadow)
        {
            ((ProcedureShadow)proc).setProcedureRegistry(this);
            return (ProcedureShadow)proc;
        }
        else if (proc instanceof IDataProducer)
            shadow = new SensorShadow(procId, (IDataProducer)proc, this);
        else
            shadow = new ProcedureShadow(procId, proc, this);
        
        return shadow;
    }


    @Override
    public void unregister(IProcedureDriver proc)
    {
        cleanup(proc, true, false);
    }
    
    
    @Override
    public void remove(String uid)
    {
        ProcedureShadow shadow = procedureShadows.get(uid);
        if (shadow == null)
            throw new IllegalArgumentException("Unknown procedure UID"); 
        IProcedureDriver proc = shadow.ref.get();
        if (proc != null)
            cleanup(proc, true, true);
    }


    protected void cleanup(IProcedureDriver proc, boolean sendEvent, boolean removeFromDb)
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);

        lock.writeLock().lock();
        String uid = proc.getUniqueIdentifier();
        log.debug("Unregistering procedure {}", uid);

        try
        {
            // if entity group, unregister members recursively
            if (proc instanceof IProcedureGroupDriver)
            {
                for (IProcedureDriver member: ((IProcedureGroupDriver<?>)proc).getMembers().values())
                    cleanup(member, false, removeFromDb);
            }

            // remove from shadow map
            ProcedureShadow shadow = procedureShadows.remove(uid);
            if (shadow != null)
                shadow.disconnectLiveProcedure(proc);

            // remove from state database
            if (removeFromDb)
                procStateDb.getProcedureStore().remove(uid);

            // publish procedure disabled event
            if (removeFromDb && sendEvent)
                eventPublisher.publish(new ProcedureRemovedEvent(System.currentTimeMillis(), shadow.getProcedureID(), shadow.getParentGroupID()));
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }


    @Override
    public IProcedureDriver getProcedureShadow(String uid)
    {
        lock.readLock().lock();

        try
        {
            // look by local ID in the module registry for backward compatibility
            ModuleRegistry moduleRegistry = hub.getModuleRegistry();
            if (moduleRegistry.isModuleLoaded(uid))
            {
                IModule<?> module = moduleRegistry.getModuleById(uid);
                if (!(module instanceof IProcedureDriver))
                    throw new IllegalArgumentException("Module " + MsgUtils.moduleString(module) + " is not a procedure");
                return getProcedureShadow(((IProcedureDriver)module).getUniqueIdentifier());
            }

            return procedureShadows.get(uid);
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


    @Override
    public ProcedureId getProcedureId(String uid)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public IProcedureStateDatabase getProcedureStateDatabase()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
