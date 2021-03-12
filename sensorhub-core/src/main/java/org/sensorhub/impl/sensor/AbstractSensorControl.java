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

import org.sensorhub.api.event.IEventHandler;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.tasking.ICommandReceiver;
import org.sensorhub.api.tasking.IStreamingControlInterface;
import org.sensorhub.impl.event.BasicEventHandler;


/**
 * <p>
 * Default implementation of common sensor control interface API methods.
 * By default, async exec, scheduling and status history are reported as
 * unsupported.
 * </p>
 *
 * @author Alex Robin
 * @param <T> Type of parent procedure
 * @since Nov 22, 2014
 */
public abstract class AbstractSensorControl<T extends ICommandReceiver> implements IStreamingControlInterface
{
    protected static final String ERROR_NO_ASYNC = "Asynchronous command processing is not supported by driver ";
    protected static final String ERROR_NO_SCHED = "Command scheduling is not supported by driver ";
    protected static final String ERROR_NO_STATUS_HISTORY = "Status history is not supported by driver ";
    protected final String name;
    protected final T parentSensor;
    protected final IEventHandler eventHandler;
    
    
    public AbstractSensorControl(T parentSensor)
    {
        this(null, parentSensor);
    }
    
    
    public AbstractSensorControl(String name, T parentSensor)
    {
        this.name = name;
        this.parentSensor = parentSensor;
        this.eventHandler = new BasicEventHandler();
    }
    
    
    @Override
    public ICommandReceiver getParentProducer()
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
    public void registerListener(IEventListener listener)
    {
        eventHandler.registerListener(listener);
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        eventHandler.unregisterListener(listener);
    }
    
}
