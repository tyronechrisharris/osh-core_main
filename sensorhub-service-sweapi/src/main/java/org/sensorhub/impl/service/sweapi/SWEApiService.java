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

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.service.IServiceModule;
import org.sensorhub.impl.database.registry.FilteredFederatedDatabase;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.service.AbstractHttpServiceModule;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.feature.FoiHistoryHandler;
import org.sensorhub.impl.service.sweapi.home.ConformanceHandler;
import org.sensorhub.impl.service.sweapi.home.HomePageHandler;
import org.sensorhub.impl.service.sweapi.obs.CustomObsFormat;
import org.sensorhub.impl.service.sweapi.obs.DataStreamHandler;
import org.sensorhub.impl.service.sweapi.obs.DataStreamSchemaHandler;
import org.sensorhub.impl.service.sweapi.obs.ObsHandler;
import org.sensorhub.impl.service.sweapi.obs.ObsStatsHandler;
import org.sensorhub.impl.service.sweapi.procedure.ProcedureDetailsHandler;
import org.sensorhub.impl.service.sweapi.procedure.ProcedureHandler;
import org.sensorhub.impl.service.sweapi.system.SystemDetailsHandler;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.service.sweapi.system.SystemHistoryHandler;
import org.sensorhub.impl.service.sweapi.system.SystemMembersHandler;
import org.sensorhub.impl.service.sweapi.task.CommandHandler;
import org.sensorhub.impl.service.sweapi.task.CommandStatusHandler;
import org.sensorhub.impl.service.sweapi.task.CommandStreamHandler;
import org.sensorhub.impl.service.sweapi.task.CommandStreamSchemaHandler;
import org.sensorhub.utils.NamedThreadFactory;


/**
 * <p>
 * Implementation of SensorHub SWE API service.<br/>
 * The service can be configured to expose some or all of the systems and
 * observations available on the hub.
 * </p>
 *
 * @author Alex Robin
 * @since Oct 12, 2020
 */
public class SWEApiService extends AbstractHttpServiceModule<SWEApiServiceConfig> implements IServiceModule<SWEApiServiceConfig>, IEventListener
{
    protected SWEApiServlet servlet;
    ScheduledExecutorService threadPool;


    @Override
    public void setConfiguration(SWEApiServiceConfig config)
    {
        super.setConfiguration(config);
        this.securityHandler = new SWEApiSecurity(this, config.security.enableAccessControl);
    }


    @Override
    protected void doStart() throws SensorHubException
    {
        IObsSystemDatabase readDb;
        IObsSystemDatabase writeDb;
        
        // get handle to obs system database
        if (config.databaseID != null)
        {
            writeDb = (IObsSystemDatabase)getParentHub().getModuleRegistry()
                .getModuleById(config.databaseID);
            if (writeDb != null && !writeDb.isOpen())
                writeDb = null;
        }
        else
            writeDb = null;
        
        // get existing or create new FilteredView from config
        if (config.exposedResources != null)
        {
            if (writeDb != null)
            {
                var obsFilter = config.exposedResources.getObsFilter();
                var cmdFilter = config.exposedResources.getCommandFilter();
                readDb = new FilteredFederatedDatabase(
                    getParentHub().getDatabaseRegistry(),
                    obsFilter, cmdFilter, new ProcedureFilter.Builder().build(), writeDb.getDatabaseNum());
            }
            else
                readDb = config.exposedResources.getFilteredView(getParentHub());
        }
        else
            readDb = getParentHub().getDatabaseRegistry().getFederatedDatabase();

        // init thread pool
        threadPool = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("SWAPool"));

        // init timeout monitor
        //timeOutMonitor = new TimeOutMonitor();
        
        // load custom formats
        Map<String, CustomObsFormat> customFormats = new HashMap<String, CustomObsFormat>();
        for (var formatConfig: config.customFormats)
        {
            try
            {
                // find impl for this mime type
                ModuleRegistry moduleReg = getParentHub().getModuleRegistry();
                var clazz = moduleReg.<CustomObsFormat>findClass(formatConfig.className);
                var formatImpl = clazz.getDeclaredConstructor().newInstance();
                customFormats.put(formatConfig.mimeType, formatImpl);
                getLogger().info("Loaded custom {} format implementation: {}", formatConfig.mimeType, formatConfig.className);
            }
            catch (Exception e)
            {
                reportError("Error while initializing custom format for " + formatConfig.mimeType, e);
            }
        }
        
        // create obs db read/write wrapper
        var db = new ObsSystemDbWrapper(readDb, writeDb, getParentHub().getDatabaseRegistry());
        var eventBus = getParentHub().getEventBus();
        var security = (SWEApiSecurity)this.securityHandler;
        var readOnly = writeDb == null || writeDb.isReadOnly();
        
        // create resource handlers hierarchy
        var homePage = new HomePageHandler(config);
        var rootHandler = new RootHandler(homePage, readOnly);
        rootHandler.addSubResource(new ConformanceHandler(config));
        
        // systems and sub-resources
        var systemsHandler = new SystemHandler(eventBus, db, security.system_summary_permissions);
        rootHandler.addSubResource(systemsHandler);
        
        var sysHistoryHandler = new SystemHistoryHandler(eventBus, db, security.system_summary_permissions);
        systemsHandler.addSubResource(sysHistoryHandler);
        
        var sysDetailsHandler = new SystemDetailsHandler(eventBus, db, security.system_details_permissions);
        systemsHandler.addSubResource(sysDetailsHandler);
        
        var sysMembersHandler = new SystemMembersHandler(eventBus, db, security.system_summary_permissions);
        systemsHandler.addSubResource(sysMembersHandler);
        
        // procedures
        if (db.getReadDb() instanceof IProcedureDatabase)
        {
            var procHandler = new ProcedureHandler(eventBus, db, security.proc_summary_permissions);
            rootHandler.addSubResource(procHandler);
            
            var procDetailsHandler = new ProcedureDetailsHandler(eventBus, db, security.proc_details_permissions);
            procHandler.addSubResource(procDetailsHandler);
        }
        
        // features of interest and sub-resources
        var foiHandler = new FoiHandler(eventBus, db, security.foi_permissions);
        rootHandler.addSubResource(foiHandler);
        systemsHandler.addSubResource(foiHandler);
        
        var foiHistoryHandler = new FoiHistoryHandler(eventBus, db, security.foi_permissions);
        foiHandler.addSubResource(foiHistoryHandler);
        
        // datastreams
        var dataStreamHandler = new DataStreamHandler(eventBus, db, security.datastream_permissions, customFormats);
        rootHandler.addSubResource(dataStreamHandler);
        systemsHandler.addSubResource(dataStreamHandler);
        var dataSchemaHandler = new DataStreamSchemaHandler(eventBus, db, security.datastream_permissions);
        dataStreamHandler.addSubResource(dataSchemaHandler);
        
        // observations
        var obsHandler = new ObsHandler(eventBus, db, threadPool, security.obs_permissions, customFormats);
        rootHandler.addSubResource(obsHandler);
        dataStreamHandler.addSubResource(obsHandler);
        foiHandler.addSubResource(obsHandler);
        
        // obs statistics
        var obsStatsHandler = new ObsStatsHandler(db, security.datastream_permissions);
        //rootHandler.addSubResource(obsStatsHandler);
        dataStreamHandler.addSubResource(obsStatsHandler);
        //foiHandler.addSubResource(obsStatsHandler);
        
        // command streams
        var cmdStreamHandler = new CommandStreamHandler(eventBus, db, security.commandstream_permissions);
        rootHandler.addSubResource(cmdStreamHandler);
        systemsHandler.addSubResource(cmdStreamHandler);
        var cmdSchemaHandler = new CommandStreamSchemaHandler(eventBus, db, security.commandstream_permissions);
        cmdStreamHandler.addSubResource(cmdSchemaHandler);
        
        // commands
        var cmdHandler = new CommandHandler(eventBus, db, threadPool, security.command_permissions);
        cmdStreamHandler.addSubResource(cmdHandler);
        
        // command status
        var statusHandler = new CommandStatusHandler(eventBus, db, threadPool, security.command_permissions);
        cmdStreamHandler.addSubResource(statusHandler);
        cmdHandler.addSubResource(statusHandler);
        
        //var cmdSchemaHandler = new CommandStreamSchemaHandler(eventBus, db, security.commands_permissions);
        //dataStreamHandler.addSubResource(dataSchemaHandler);
        
        // sampled features
        /*var featureStore = new FeatureStoreWrapper(
            featureReadDatabase.getFeatureStore(), 
            featureWriteDatabase != null ? featureWriteDatabase.getFeatureStore() : null);
        FeatureHandler featureHandler = new FeatureHandler(featureStore);
        rootHandler.addSubResource(featureHandler);*/
        
        // deploy servlet
        servlet = new SWEApiServlet(this, (SWEApiSecurity)securityHandler, rootHandler, getLogger());
        deploy();

        setState(ModuleState.STARTED);
    }


    protected void deploy() throws SensorHubException
    {
        var wildcardEndpoint = config.endPoint + "/*";
        
        // deploy ourself to HTTP server
        httpServer.deployServlet(servlet, wildcardEndpoint);
        httpServer.addServletSecurity(wildcardEndpoint, config.security.requireAuth);
    }


    @Override
    protected void doStop()
    {
        // undeploy servlet
        undeploy();
        
        // stop thread pool
        if (threadPool != null)
            threadPool.shutdown();

        setState(ModuleState.STOPPED);
    }


    protected void undeploy()
    {
        // return silently if HTTP server missing on stop
        if (httpServer == null || !httpServer.isStarted())
            return;

        if (servlet != null)
        {
            httpServer.undeployServlet(servlet);
            servlet.destroy();
            servlet = null;
        }
    }


    @Override
    public void cleanup() throws SensorHubException
    {
        // unregister security handler
        if (securityHandler != null)
            securityHandler.unregister();
    }


    public ScheduledExecutorService getThreadPool()
    {
        return threadPool;
    }
    
    
    public SWEApiServlet getServlet()
    {
        return servlet;
    }


    /*public TimeOutMonitor getTimeOutMonitor()
    {
        return timeOutMonitor;
    }*/
}
