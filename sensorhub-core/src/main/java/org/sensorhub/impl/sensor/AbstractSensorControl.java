/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor;

import java.util.concurrent.CompletableFuture;
import org.sensorhub.api.command.CommandStatus;
import org.sensorhub.api.command.CommandException;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandReceiver;
import org.sensorhub.api.command.IStreamingControlInterface;
import org.sensorhub.api.event.IEventHandler;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.sensor.SensorException;
import org.sensorhub.impl.event.BasicEventHandler;
import org.sensorhub.impl.module.AbstractModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Default implementation of common sensor control interface API methods.
 * By default, async exec, scheduling and status history are reported as
 * unsupported.
 * </p>
 *
 * @author Alex Robin
 * @param <T> Type of parent system
 * @since Nov 22, 2014
 */
public abstract class AbstractSensorControl<T extends ICommandReceiver> implements IStreamingControlInterface
{
    protected final T parentSensor;
    protected final IEventHandler eventHandler;
    protected final String name;
    protected final Logger log;
    
    
    public AbstractSensorControl(String name, T parentSensor)
    {
        this(name, (T)parentSensor, null);
    }
    
    
    /**
     * Constructs a new control input with the given name and attached to the
     * provided parent sensor.<br/>
     * @param name
     * @param parentSensor
     * @param eventSrcInfo
     * @param log
     */
    public AbstractSensorControl(String name, T parentSensor, Logger log)
    {
        this.name = Asserts.checkNotNull(name, "name");
        this.parentSensor = Asserts.checkNotNull(parentSensor, "parentSensor");
        this.eventHandler = new BasicEventHandler();
        
        // setup logger
        if (log == null)
        {
            if (log == null && parentSensor instanceof AbstractModule)
                this.log = ((AbstractModule<?>)parentSensor).getLogger();
            else
                this.log = LoggerFactory.getLogger(getClass().getCanonicalName());
        }
        else
            this.log = log;
    }
    
    
    @Override
    public T getParentProducer()
    {
        return parentSensor;
    }
    
    
    @Override
    public String getName()
    {
        return name;
    }


    @Override
    public boolean isEnabled()
    {
        return true;
    }


    @Override
    public CompletableFuture<ICommandStatus> submitCommand(ICommandData command)
    {
        return CompletableFuture.supplyAsync(() -> {
            ICommandStatus status;
            try
            {
                var ok = execCommand(command.getParams());
                status = ok ? CommandStatus.completed(command.getID()) : CommandStatus.failed(command.getID(), null);
            }
            catch (CommandException e)
            {
                getLogger().error("Error processing command", e);
                status = CommandStatus.failed(command.getID(), e.getMessage());
            }
            
            return status;
        });
    }


    @Override
    public void validateCommand(ICommandData command) throws CommandException
    {        
    }
    
        
    /**
     * Helper method to implement simple synchronous command logic, backward compatible
     * with existing driver implementations (1.x). For more advanced implementations,
     * override {@link #submitCommand(ICommandData)} directly.
     * @param cmdData
     * @return
     * @throws SensorException
     */
    protected boolean execCommand(DataBlock cmdData) throws CommandException
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public void registerListener(IEventListener listener)
    {
        eventHandler.registerListener(listener);
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        eventHandler.unregisterListener(listener);
    }
    
    
    protected Logger getLogger()
    {
        return log; 
    }
    
}
