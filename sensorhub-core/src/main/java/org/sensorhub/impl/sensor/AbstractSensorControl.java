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

import java.util.List;
import java.util.UUID;
import net.opengis.swe.v20.DataBlock;
import org.sensorhub.api.common.CommandStatus;
import org.sensorhub.api.data.ICommandReceiver;
import org.sensorhub.api.data.IStreamingControlInterface;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventHandler;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.api.common.CommandStatus.StatusCode;
import org.sensorhub.api.sensor.SensorException;
import org.sensorhub.impl.event.BasicEventHandler;
import org.sensorhub.impl.event.EventSourceInfo;


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
    protected final IEventSourceInfo eventSrcInfo;
    
    
    public AbstractSensorControl(T parentSensor)
    {
        this(null, parentSensor);
    }
    
    
    public AbstractSensorControl(String name, T parentSensor)
    {
        this.name = name;
        this.parentSensor = parentSensor;
        
        // use event handler of the parent sensor
        this.eventHandler = new BasicEventHandler();
        String groupID = parentSensor.getUniqueIdentifier();
        String sourceID = EventUtils.getProcedureControlSourceID(groupID, getName());
        this.eventSrcInfo = new EventSourceInfo(groupID, sourceID);
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
    public CommandStatus execCommandGroup(List<DataBlock> commands) throws SensorException
    {
        CommandStatus groupStatus = new CommandStatus();
        groupStatus.id = UUID.randomUUID().toString();
        groupStatus.status = StatusCode.COMPLETED;
        
        // if any of the commands fail, return fail status with
        // error message and don't process more commands
        for (DataBlock cmd: commands)
        {
            CommandStatus cmdStatus = execCommand(cmd);
            if (cmdStatus.status == StatusCode.REJECTED || cmdStatus.status == StatusCode.FAILED)
            {
                groupStatus.status = cmdStatus.status;
                groupStatus.message = cmdStatus.message;
                break;
            }          
        }
        
        return groupStatus;
    }
    
}
