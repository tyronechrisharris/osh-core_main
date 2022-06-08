/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.processing;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataComponent;
import org.sensorhub.api.command.IStreamingControlInterface;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.processing.IProcessModule;
import org.sensorhub.api.processing.ProcessConfig;
import org.sensorhub.api.processing.ProcessingException;
import org.sensorhub.api.system.ISystemDriver;
import org.sensorhub.api.system.ISystemGroupDriver;
import org.sensorhub.impl.module.AbstractModule;
import org.vast.ogc.gml.IFeature;
import org.vast.sensorML.AggregateProcessImpl;


/**
 * <p>
 * Base class for processing modules
 * </p>
 * 
 * @param <T> Type of process module config
 *
 * @author Alex Robin
 * @since June 7, 2022
 */
public abstract class AbstractProcessModule<T extends ProcessConfig> extends AbstractModule<T> implements IProcessModule<T>
{
    protected Map<String, DataComponent> inputs = new LinkedHashMap<>();
    protected Map<String, DataComponent> outputs = new LinkedHashMap<>();
    protected Map<String, DataComponent> parameters = new LinkedHashMap<>();
    protected Map<String, IStreamingDataInterface> outputInterfaces = new LinkedHashMap<>();
    protected Map<String, IStreamingControlInterface> controlInterfaces = new LinkedHashMap<>();

    protected volatile ISystemGroupDriver<?> parentSystem;
    protected AggregateProcessImpl processDescription;
    protected long lastUpdatedProcess = Long.MIN_VALUE;
    protected boolean paused = false;
    protected int errorCount = 0;
    
    
    public AbstractProcessModule()
    {
    }
    
    
    @Override
    protected void beforeStart() throws SensorHubException
    {
        super.beforeStart();
        
        // register sensor with registry if attached to a hub and we have no parent
        try
        {
            if (hasParentHub() && getParentHub().getSystemDriverRegistry() != null)
                getParentHub().getSystemDriverRegistry().register(this).get(); // for now, block here until init is also async
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new ProcessingException("Error registering process", e.getCause());
        }
    }
    
    
    @Override
    protected void afterStop() throws SensorHubException
    {
        // unregister process if attached to a hub
        try
        {
            if (hasParentHub() && getParentHub().getSystemDriverRegistry() != null && parentSystem == null)
                getParentHub().getSystemDriverRegistry().unregister(this).get();
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new ProcessingException("Error unregistering process", e.getCause());
        }
        
        super.afterStop();
    }


    @Override
    public Map<String, DataComponent> getInputs()
    {
        return Collections.unmodifiableMap(inputs);
    }
    
    
    @Override
    public Map<String, DataComponent> getParameters()
    {
        // for parameters we actually maintain a buffer so they
        // can be set during process execution
        return Collections.unmodifiableMap(parameters);
    }
    
    
    @Override
    public Map<String, IStreamingDataInterface> getOutputs()
    {
        return Collections.unmodifiableMap(outputInterfaces);
    }


    @Override
    public Map<String, ? extends IStreamingControlInterface> getCommandInputs()
    {
        return Collections.unmodifiableMap(controlInterfaces);
    }
    
    
    @Override
    public String getUniqueIdentifier()
    {
        return processDescription.getUniqueIdentifier();
    }


    @Override
    public synchronized AbstractProcess getCurrentDescription()
    {
        return processDescription;
    }


    @Override
    public synchronized long getLatestDescriptionUpdate()
    {
        return lastUpdatedProcess;
    }


    @Override
    public String getParentSystemUID()
    {
        return parentSystem != null ? parentSystem.getUniqueIdentifier() : null;
    }


    @Override
    public ISystemGroupDriver<? extends ISystemDriver> getParentSystem()
    {
        return parentSystem;
    }
    
    
    public void attachToParent(ISystemGroupDriver<? extends ISystemDriver> parentSystem)
    {
        this.parentSystem = parentSystem;
    }


    @Override
    public boolean isEnabled()
    {
        return isStarted();
    }


    @Override
    public void cleanup()
    {
    }


    @Override
    public Map<String, ? extends IFeature> getCurrentFeaturesOfInterest()
    {
        return Collections.emptyMap();
    }
}
