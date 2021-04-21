/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.swe;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.module.ModuleEvent;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.service.IServiceModule;
import org.sensorhub.impl.database.registry.FilteredFederatedObsDatabase;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.impl.service.HttpServer;
import org.sensorhub.utils.NamedThreadFactory;


/**
 * <p>
 * Base abstract class for both SOS and SPS service modules
 * </p>
 * 
 * @param <ConfigType> Type of config
 *
 * @author Alex Robin
 * @since Mar 31, 2021
 */
public abstract class SWEService<ConfigType extends SWEServiceConfig> extends AbstractModule<ConfigType> implements IServiceModule<ConfigType>, IEventListener
{
    protected SWEServlet servlet;
    protected ScheduledExecutorService threadPool;
    protected IProcedureObsDatabase readDatabase;
    protected IProcedureObsDatabase writeDatabase;
    
    
    protected abstract IQueryFilter getResourceFilter();
 
    
    @Override
    public void start() throws SensorHubException
    {
        if (canStart())
        {
            HttpServer httpServer = HttpServer.getInstance();
            if (httpServer == null)
                throw new SensorHubException("HTTP server module is not loaded");

            // subscribe to server lifecycle events
            httpServer.registerListener(this);

            // we actually start in the handleEvent() method when
            // a STARTED event is received from HTTP server
        }
    }    
    

    @Override
    protected void doStart() throws SensorHubException
    {
        // get handle to write database
        // use the configured database
        if (config.databaseID != null)
        {
            writeDatabase = (IProcedureObsDatabase)getParentHub().getModuleRegistry()
                .getModuleById(config.databaseID);
        }
        
        // or default to the procedure state DB
        else
        {
            writeDatabase = getParentHub().getProcedureRegistry().getProcedureStateDatabase();
        }
        
        // if a filter was provided, use a filtered db implementation
        var filter = getResourceFilter();
        if (filter != null)
        {
            if (writeDatabase != null)
            {
                // if a writable database was provided, make sure we always expose
                // its content via this service by flagging it as unfiltered
                if (filter instanceof ObsFilter)
                {
                    readDatabase = new FilteredFederatedObsDatabase(
                        getParentHub().getDatabaseRegistry(),
                        (ObsFilter)filter, writeDatabase.getDatabaseNum());
                }
                else if (filter instanceof CommandFilter)
                {
                    readDatabase = new FilteredFederatedObsDatabase(
                        getParentHub().getDatabaseRegistry(),
                        (CommandFilter)filter, writeDatabase.getDatabaseNum());
                }
                else
                    throw new IllegalStateException();
            }
            else
                readDatabase = config.exposedResources.getFilteredView(getParentHub());
        }
        
        // else expose all procedures on this hub
        else
            readDatabase = getParentHub().getDatabaseRegistry().getFederatedObsDatabase();

        // init thread pool
        var threadPool = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("SOSPool"));
        threadPool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.threadPool = threadPool;
    }


    @Override
    protected void doStop() throws SensorHubException
    {
        // undeploy servlet
        undeploy();
        if (servlet != null)
            servlet.destroy();
        servlet = null;
        
        // stop thread pool
        if (threadPool != null)
            threadPool.shutdown();

        setState(ModuleState.STOPPED);
    }


    protected void deploy() throws SensorHubException
    {
        HttpServer httpServer = HttpServer.getInstance();
        if (httpServer == null || !httpServer.isStarted())
            throw new SensorHubException("An HTTP server instance must be started");

        // deploy ourself to HTTP server
        httpServer.deployServlet(servlet, config.endPoint);
        httpServer.addServletSecurity(config.endPoint, config.security.requireAuth);
    }


    protected void undeploy()
    {
        HttpServer httpServer = HttpServer.getInstance();

        // return silently if HTTP server missing on stop
        if (httpServer == null || !httpServer.isStarted())
            return;

        httpServer.undeployServlet(servlet);
    }


    @Override
    public void cleanup() throws SensorHubException
    {
        // stop listening to http server events
        HttpServer httpServer = HttpServer.getInstance();
        if (httpServer != null)
            httpServer.unregisterListener(this);

        // TODO destroy all virtual sensors?
        //for (SOSConsumerConfig consumerConf: config.dataConsumers)
        //    SensorHub.getInstance().getModuleRegistry().destroyModule(consumerConf.sensorID);

        // unregister security handler
        if (securityHandler != null)
            securityHandler.unregister();
    }


    @Override
    public void handleEvent(Event e)
    {
        // catch HTTP server lifecycle events
        if (e instanceof ModuleEvent && e.getSource() == HttpServer.getInstance())
        {
            ModuleState newState = ((ModuleEvent) e).getNewState();

            // start when HTTP server is enabled
            if (newState == ModuleState.STARTED)
            {
                try
                {
                    doStart();
                }
                catch (Exception ex)
                {
                    reportError("Service could not start", ex);
                }
            }

            // stop when HTTP server is disabled
            else if (newState == ModuleState.STOPPED)
            {
                try
                {
                    doStop();
                }
                catch (SensorHubException ex)
                {
                    reportError("Service could not stop", ex);
                }
            }
        }
    }


    public ScheduledExecutorService getThreadPool()
    {
        return threadPool;
    }


    public IProcedureObsDatabase getReadDatabase()
    {
        return readDatabase;
    }


    public IProcedureObsDatabase getWriteDatabase()
    {
        return writeDatabase;
    }
    
}
