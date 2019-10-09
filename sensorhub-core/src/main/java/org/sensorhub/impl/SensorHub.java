/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl;

import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.ISensorHubConfig;
import org.sensorhub.api.comm.INetworkManager;
import org.sensorhub.api.datastore.IObsDatabaseRegistry;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.module.IModuleConfigRepository;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.processing.IProcessingManager;
import org.sensorhub.api.security.ISecurityManager;
import org.sensorhub.impl.comm.NetworkManagerImpl;
import org.sensorhub.impl.datastore.registry.FederatedDatabaseRegistry;
import org.sensorhub.impl.event.EventBus;
import org.sensorhub.impl.module.InMemoryConfigDb;
import org.sensorhub.impl.module.ModuleConfigJsonFile;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.procedure.DefaultProcedureRegistry;
import org.sensorhub.impl.processing.ProcessingManagerImpl;
import org.sensorhub.impl.security.ClientAuth;
import org.sensorhub.impl.security.SecurityManagerImpl;
import org.sensorhub.utils.ModuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Main class reponsible for starting/stopping all modules
 * </p>
 *
 * @author Alex Robin
 * @since Sep 4, 2013
 */
public class SensorHub implements ISensorHub
{
    private static final Logger log = LoggerFactory.getLogger(SensorHub.class);    
    private static final String ERROR_MSG = "Fatal error during sensorhub execution";
    
    private ISensorHubConfig config;
    private ModuleRegistry moduleRegistry;
    private IEventBus eventBus;
    private IProcedureRegistry procedureRegistry;
    private IObsDatabaseRegistry databaseRegistry;
    private INetworkManager networkManager;
    private ISecurityManager securityManager;
    private IProcessingManager processingManager;
    private volatile boolean stopped;
    
    
    public SensorHub()
    {
        IModuleConfigRepository configDB = new InMemoryConfigDb();
        init(new SensorHubConfig(),
            new ModuleRegistry(this, configDB),
            new EventBus());
    }
    
    
    public SensorHub(ISensorHubConfig config)
    {
        IModuleConfigRepository configDB = new ModuleConfigJsonFile(config.getModuleConfigPath(), true);
        init(config,
            new ModuleRegistry(this, configDB),
            new EventBus());
    }
    
    
    protected void init(ISensorHubConfig config, ModuleRegistry registry, IEventBus eventBus)
    {
        this.config = config;
        this.eventBus = eventBus;
        this.moduleRegistry = registry;
        this.databaseRegistry = new FederatedDatabaseRegistry(this);
        this.procedureRegistry = new DefaultProcedureRegistry(this);
    }   
    
    
    @Override
    public void start()
    {
        log.info("*****************************************");
        log.info("Starting SensorHub...");
        
        // prepare client authenticator (e.g. for HTTP connections, etc...)
        ClientAuth.createInstance("keystore");
                
        // load all modules in the order implied by dependency constraints
        moduleRegistry.loadAllModules();
    }
    
    
    @Override
    public void saveAndStop()
    {
        stop(true, true);
    }
    
    
    @Override
    public void stop()
    {
        stop(false, true);
    }
    
    
    @Override
    public synchronized void stop(boolean saveConfig, boolean saveState)
    {
        try
        {
            if (!stopped)
            {
                moduleRegistry.shutdown(saveConfig, saveState);
                eventBus.shutdown();
                stopped = true;
                log.info("SensorHub was cleanly stopped");
            }
        }
        catch (Exception e)
        {
            log.error("Error while stopping SensorHub", e);
        }
    }
    
    
    @Override
    public ISensorHubConfig getConfig()
    {
        return config;
    }


    public void setConfig(ISensorHubConfig config)
    {
        this.config = config;
    }


    @Override
    public ModuleRegistry getModuleRegistry()
    {
        return moduleRegistry;
    }
    
    
    @Override
    public IEventBus getEventBus()
    {
        return eventBus;
    }
    
    
    @Override
    public IProcedureRegistry getProcedureRegistry()
    {
        return procedureRegistry;
    }
    
    
    @Override
    public IObsDatabaseRegistry getDatabaseRegistry()
    {
        return databaseRegistry;
    }
    
    
    @Override
    public synchronized INetworkManager getNetworkManager()
    {
        if (networkManager == null)
            networkManager = new NetworkManagerImpl(this);
        return networkManager;
    }
    
    
    @Override
    public synchronized ISecurityManager getSecurityManager()
    {
        if (securityManager == null)
            securityManager = new SecurityManagerImpl(this);
        return securityManager;
    }
    
    
    @Override
    public synchronized IProcessingManager getProcessingManager()
    {
        if (processingManager == null)
            processingManager = new ProcessingManagerImpl(this);
        return processingManager;
    }
    
    
    public static void main(String[] args)
    {
        // if no arg provided
        if (args.length < 2)
        {
            String version = ModuleUtils.getModuleInfo(SensorHub.class).getModuleVersion();
            String buildNumber = ModuleUtils.getBuildNumber(SensorHub.class);
            
            // print usage
            System.out.println("SensorHub " + version + " (build " + buildNumber + ")");
            System.out.println("Command syntax: sensorhub [module_config_path] [base_storage_path]");
            System.exit(1);
        }
        
        // start sensorhub
        ISensorHub instance = null;
        try
        {
            SensorHubConfig config = new SensorHubConfig(args[0], args[1]);
            instance = new SensorHub(config);
                        
            // register shutdown hook for a clean stop 
            final ISensorHub sh = instance;
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run()
                {
                    sh.stop();                    
                }            
            });
            
            instance.start();
        }
        catch (Exception e)
        {
            if (instance != null)
                instance.stop();
            
            System.err.println(ERROR_MSG);
            System.err.println(e.getLocalizedMessage());
            log.error(ERROR_MSG, e);
            
            System.exit(2);
        }
    }
}
