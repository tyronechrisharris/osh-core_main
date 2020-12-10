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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.IStreamingControlInterface;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.database.DatabaseConfig;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.database.IProcedureStateDatabase;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.api.procedure.IProcedureDriver;
import org.sensorhub.api.procedure.IProcedureEventHandlerDatabase;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.impl.database.obs.ProcedureObsEventDatabase;
import org.sensorhub.impl.database.obs.ProcedureObsEventDatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.ogc.gml.IGeoFeature;
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
    ProcedureObsEventDatabase procStateDb;
    IProcedureObsDatabase federatedDb;
    ReadWriteLock lock = new ReentrantReadWriteLock();
    Map<String, ProcedureRegistryEventHandler> driverHandlers = new ConcurrentSkipListMap<>();


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
            ProcedureObsEventDatabaseConfig dbListenerConfig = new ProcedureObsEventDatabaseConfig();
            dbListenerConfig.databaseNum = 0;
            dbListenerConfig.dbConfig = stateDbConfig;

            procStateDb = new ProcedureObsEventDatabase();
            procStateDb.setParentHub(hub);
            procStateDb.init(dbListenerConfig);
            procStateDb.start();
            
            hub.getDatabaseRegistry().register(procStateDb);            
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Error initializing procedure state database", e);
        }
    }
    
    
    protected ProcedureRegistryEventHandler createDriverHandler(IProcedureDriver proc)
    {
        var procUID = proc.getUniqueIdentifier();
        
        // get DB handling this procedure
        // this call with return the dedicated DB if available, or the default state DB
        IProcedureObsDatabase db = hub.getDatabaseRegistry().getObsDatabase(procUID);
        
        // error if DB is not an event handler DB
        if (!(db instanceof IProcedureEventHandlerDatabase))
            throw new IllegalStateException("Another database already contains a procedure with UID " + procUID);
        
        log.info("Procedure " + procUID + " handled by DB #" + db.getDatabaseID());
        return new ProcedureRegistryEventHandler(this, proc, (IProcedureEventHandlerDatabase)db);
    }


    public CompletableFuture<Boolean> register(IProcedureDriver proc)
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);
        String procUID = OshAsserts.checkValidUID(proc.getUniqueIdentifier());
        log.debug("Registering procedure {}", procUID);
        
        // create listener or simply reconnect to it
        // use compute() to do it atomatically
        var handler = driverHandlers.compute(procUID, (k,v) -> {
            if (v != null)
            {
                IProcedureDriver liveProc = v.driverRef.get();
                if (liveProc != null && liveProc != proc)
                    throw new IllegalArgumentException("A procedure with UID " + procUID + " is already registered");
                return v;
            }
            else if (proc.getParentGroupUID() != null)
            {
                // create handler within parent handler
                var parentHandler = getDriverHandler(proc.getParentGroupUID());
                return parentHandler.createMemberProcedureHandler(proc);
            }
            else
                return createDriverHandler(proc);
        });
        
        // connect and register procedure
        // callee will take care of double registrations
        synchronized (handler)
        {
            return handler.connect(proc);
        }
    }
    
    
    public boolean isRegistered(String uid)
    {
        return driverHandlers.containsKey(uid);
    }
    
    
    protected ProcedureRegistryEventHandler getDriverHandler(String procUID)
    {
        var handler = driverHandlers.get(procUID);
        if (handler == null)
            throw new IllegalStateException("Procedure " + procUID + " is not registered");        
        
        return handler;
    }


    @Override
    public CompletableFuture<Void> unregister(IProcedureDriver proc)
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);
        String procUID = OshAsserts.checkValidUID(proc.getUniqueIdentifier());
        
        var handler = driverHandlers.remove(procUID);
        if (handler != null)
            return handler.unregister(proc);
        else
            return CompletableFuture.completedFuture(null);
    }
    
    
    @Override
    public CompletableFuture<Boolean> register(IStreamingDataInterface dataStream)
    {
        Asserts.checkNotNull(dataStream, IStreamingDataInterface.class);
        var proc = Asserts.checkNotNull(dataStream.getParentProducer(), IProcedureDriver.class);
        var procUID = Asserts.checkNotNull(proc.getUniqueIdentifier());
        
        return getDriverHandler(procUID).register(dataStream);
    }
    
    
    @Override
    public CompletableFuture<Boolean> register(IStreamingControlInterface controlStream)
    {
        Asserts.checkNotNull(controlStream, IStreamingControlInterface.class);
        var proc = Asserts.checkNotNull(controlStream.getParentProducer(), IProcedureDriver.class);
        var procUID = OshAsserts.checkValidUID(proc.getUniqueIdentifier());
        
        return getDriverHandler(procUID).register(controlStream);
    }
    
    
    @Override
    public CompletableFuture<Boolean> register(IProcedureDriver proc, IGeoFeature foi)
    {
        Asserts.checkNotNull(proc, IProcedureDriver.class);
        Asserts.checkNotNull(foi, IGeoFeature.class);
        var procUID = OshAsserts.checkValidUID(proc.getUniqueIdentifier());
        
        return getDriverHandler(procUID).register(foi);
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T extends IProcedureDriver> WeakReference<T> getProcedure(String uid)
    {
        return (WeakReference<T>)getDriverHandler(uid).driverRef;
    }


    @Override
    public ISensorHub getParentHub()
    {
        return hub;
    }


    @Override
    public IProcedureStateDatabase getProcedureStateDatabase()
    {
        return (IProcedureStateDatabase)procStateDb.getWrappedDatabase();
    }


    protected IEventPublisher getEventPublisher()
    {
        return eventPublisher;
    }

}
