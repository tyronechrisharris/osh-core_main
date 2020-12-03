/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import org.sensorhub.api.event.Event;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.module.ModuleEvent;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.service.IServiceModule;
import org.sensorhub.impl.datastore.view.ProcedureObsDatabaseView;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.impl.service.HttpServer;
import org.sensorhub.utils.NamedThreadFactory;
import org.vast.ows.sos.SOSServiceCapabilities;
import com.google.common.base.Strings;


/**
 * <p>
 * Implementation of SensorHub generic SOS service.
 * This service is automatically configured (mostly) from information obtained
 * from the selected data sources (sensors, storages, processes, etc).
 * </p>
 *
 * @author Alex Robin
 * @since Sep 7, 2013
 */
public class SOSService extends AbstractModule<SOSServiceConfig> implements IServiceModule<SOSServiceConfig>, IEventListener
{
    protected SOSServlet servlet;
    ScheduledExecutorService threadPool;
    TimeOutMonitor timeOutMonitor;
    IProcedureObsDatabase readDatabase;
    IProcedureObsDatabase writeDatabase;


    @Override
    public void requestStart() throws SensorHubException
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
    public void init() throws SensorHubException
    {
        super.init();
        
        // validate config
        for (var providerConfig: config.customDataProviders)
        {
            if (Strings.isNullOrEmpty(providerConfig.procedureUID))
                throw new SensorHubException("Provider configuration must specify a procedure unique ID");
        }
        
        for (var formatConfig: config.customFormats)
        {
            if (Strings.isNullOrEmpty(formatConfig.mimeType))
                throw new SensorHubException("Custum format must specify a mime type");
        }        
        
        this.securityHandler = new SOSSecurity(this, config.security.enableAccessControl);
    }


    @Override
    public void start() throws SensorHubException
    {
        // get handle to database
        if (config.databaseID != null)
        {
            writeDatabase = (IProcedureObsDatabase)getParentHub().getModuleRegistry()
                .getModuleById(config.databaseID);
        }
        
        // if exposed resource filter is set, get FilteredView from config
        if (config.exposedResources != null)
        {
            readDatabase = config.exposedResources.getFilteredView(getParentHub());
        }
        
        // else if some providers are configured, expose these only
        else if (config.customDataProviders != null && !config.customDataProviders.isEmpty())
        {
            var procUIDs = config.customDataProviders.stream()
                .map(config -> config.procedureUID)
                .collect(Collectors.toSet());
            
            var includeFilter = new ObsFilter.Builder()
                .withProcedures().withUniqueIDs(procUIDs).done()
                .build();
            
            readDatabase = new ProcedureObsDatabaseView(
                getParentHub().getDatabaseRegistry().getFederatedObsDatabase(),
                includeFilter);
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

        // init timeout monitor
        timeOutMonitor = new TimeOutMonitor(threadPool);

        // deploy servlet
        servlet = new SOSServlet(this, (SOSSecurity)this.securityHandler, getLogger());
        deploy();

        setState(ModuleState.STARTED);
    }


    @Override
    public void stop()
    {
        // undeploy servlet
        undeploy();
        if (servlet != null)
            servlet.stop();
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
                    start();
                }
                catch (Exception ex)
                {
                    reportError("SOS Service could not start", ex);
                }
            }

            // stop when HTTP server is disabled
            else if (newState == ModuleState.STOPPED)
                stop();
        }
    }


    public SOSServiceCapabilities getCapabilities()
    {
        if (isStarted())
            return servlet.updateCapabilities();
        else
            return null;
    }


    public ScheduledExecutorService getThreadPool()
    {
        return threadPool;
    }


    public TimeOutMonitor getTimeOutMonitor()
    {
        return timeOutMonitor;
    }


    public SOSServlet getServlet()
    {
        return servlet;
    }


    public IProcedureObsDatabase getReadDatabase()
    {
        return readDatabase;
    }
}
