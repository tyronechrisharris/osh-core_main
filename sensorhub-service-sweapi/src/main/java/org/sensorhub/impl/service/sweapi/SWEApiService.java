/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import org.sensorhub.api.event.Event;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.module.ModuleEvent;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.service.IServiceModule;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.impl.service.HttpServer;
import org.sensorhub.utils.NamedThreadFactory;


/**
 * <p>
 * Implementation of SensorHub SWE API service.<br/>
 * The service can be configured to expose some or all of the procedures and
 * observations available on the hub.
 * </p>
 *
 * @author Alex Robin
 * @since Oct 12, 2020
 */
public class SWEApiService extends AbstractModule<SWEApiServiceConfig> implements IServiceModule<SWEApiServiceConfig>, IEventListener
{
    protected SWEApiServlet servlet;
    ExecutorService threadPool;
    //TimeOutMonitor timeOutMonitor;
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
    public void setConfiguration(SWEApiServiceConfig config)
    {
        super.setConfiguration(config);
        this.securityHandler = new SWEApiSecurity(this, config.security.enableAccessControl);
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
        
        // get existing or create new FilteredView from config
        if (config.exposedResources != null)
            readDatabase = config.exposedResources.getFilteredView(getParentHub());
        else
            readDatabase = getParentHub().getDatabaseRegistry().getFederatedObsDatabase();

        // init thread pool
        threadPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("SWEApiThreadPool"));

        // init timeout monitor
        //timeOutMonitor = new TimeOutMonitor();

        // create resource handlers hierarchy
        RootHandler rootHandler = new RootHandler();
        
        var procedureStore = new ProcedureStoreWrapper(
            readDatabase.getProcedureStore(), 
            writeDatabase != null ? writeDatabase.getProcedureStore() : null);
        ProcedureHandler procedureHandler = new ProcedureHandler(procedureStore);    
        rootHandler.addSubResource(procedureHandler);
        
        var dataStreamStore = new DataStreamStoreWrapper(
            readDatabase.getDataStreamStore(), 
            writeDatabase != null ? writeDatabase.getDataStreamStore() : null);
        DataStreamHandler dataStreamHandler = new DataStreamHandler(dataStreamStore);
        rootHandler.addSubResource(dataStreamHandler);
        procedureHandler.addSubResource(dataStreamHandler);
        
        var obsStore = new ObsStoreWrapper(
            readDatabase.getObservationStore(), 
            writeDatabase != null ? writeDatabase.getObservationStore() : null);
        ObsHandler obsHandler = new ObsHandler(obsStore);    
        rootHandler.addSubResource(obsHandler);
        dataStreamHandler.addSubResource(obsHandler);
        
        var foiStore = new FoiStoreWrapper(
            readDatabase.getFoiStore(), 
            writeDatabase != null ? writeDatabase.getFoiStore() : null);
        FoiHandler foiHandler = new FoiHandler(foiStore);
        rootHandler.addSubResource(foiHandler);
        
        // deploy servlet
        servlet = new SWEApiServlet(rootHandler, (SWEApiSecurity)securityHandler, getLogger());
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

        setState(ModuleState.STOPPED);
    }


    protected void deploy() throws SensorHubException
    {
        HttpServer httpServer = HttpServer.getInstance();
        if (httpServer == null || !httpServer.isStarted())
            throw new SensorHubException("An HTTP server instance must be started");

        // deploy ourself to HTTP server
        httpServer.deployServlet(servlet, config.endPoint + "/*");
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
                    reportError("SWE API Service could not start", ex);
                }
            }

            // stop when HTTP server is disabled
            else if (newState == ModuleState.STOPPED)
                stop();
        }
    }


    public ExecutorService getThreadPool()
    {
        return threadPool;
    }


    /*public TimeOutMonitor getTimeOutMonitor()
    {
        return timeOutMonitor;
    }*/
}
