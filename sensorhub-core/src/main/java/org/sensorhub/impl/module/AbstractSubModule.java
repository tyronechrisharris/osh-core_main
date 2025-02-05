/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.module;

import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.IModuleStateManager;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.utils.ModuleUtils;
import org.sensorhub.utils.MsgUtils;


/**
 * <p>
 * Lighter implementation of IModule for submodule classes such as
 * comm providers, message queues, etc. 
 * </p>
 *
 * @author Alex Robin
 * @param <ConfigType> Type of module config
 * @since Feb 4, 2025
 */
public abstract class AbstractSubModule<ConfigType extends ModuleConfig> implements IModule<ConfigType>
{
    protected ConfigType config;
    

    public AbstractSubModule() {}
    
    
    @Override
    public void setParentHub(ISensorHub hub)
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public ISensorHub getParentHub()
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public String getName()
    {
        return config != null && config.name != null ?
            config.name :
            getClass().getSimpleName();
    }
    
    
    @Override
    public String getDescription()
    {
        return config != null ? config.description : null;
    }


    @Override
    public String getLocalID()
    {
        return config != null && config.id != null ?
            config.id :
            ModuleUtils.NO_ID_FLAG;
    }
    
    
    @Override
    public ConfigType getConfiguration()
    {
        return config;
    }
    
    
    @Override
    public void setConfiguration(ConfigType config)
    {
        this.config = config;
    }
    
    
    @Override
    public void updateConfig(ConfigType config) throws SensorHubException
    {
        setConfiguration(config);
    }
    
    
    @Override
    public boolean isInitialized()
    {
        throw new UnsupportedOperationException();
    }

    
    @Override
    public boolean isStarted()
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public ModuleState getCurrentState()
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public boolean waitForState(ModuleState state, long timeout) throws SensorHubException
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public Throwable getCurrentError()
    {
        return null;
    }
    
    
    @Override
    public String getStatusMessage()
    {
        return null;
    }
    
    
    @Override
    public void init() throws SensorHubException
    {
    }
    
    
    @Override
    public void init(ConfigType config)
    {
        this.config = config;
    }
    

    @Override
    public void saveState(IModuleStateManager saver) throws SensorHubException
    {
        // does nothing in the default implementation        
    }


    @Override
    public void loadState(IModuleStateManager loader) throws SensorHubException
    {
        // does nothing in the default implementation
    }


    @Override
    public void cleanup() throws SensorHubException
    {
    }


    @Override
    public void registerListener(IEventListener listener)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public String toString()
    {
        return MsgUtils.moduleString(this);
    }
}
