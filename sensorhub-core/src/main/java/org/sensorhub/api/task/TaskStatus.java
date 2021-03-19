/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.task;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import org.sensorhub.api.command.CommandAck;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Simple data structure to hold status information for a command
 * </p>
 *
 * @author Alex Robin
 * @since Nov 5, 2010
 */
public class TaskStatus implements ITaskStatus
{    
	protected UUID taskID;
	protected TaskStatusCode statusCode;
    protected ReasonCode reasonCode;
	protected Instant updateTime;
	protected TimeExtent estimatedExecTime;
    protected Duration estimatedDuration;
    protected Collection<CommandAck> commandsStatus;
    protected String message;
	
	
	public TaskStatus(TaskStatusCode statusCode)
	{
	    this.statusCode = statusCode;
	}
	
	
	public TaskStatus(UUID taskID, TaskStatusCode statusCode)
	{
	    this(statusCode);
	    this.taskID = taskID;
	    this.updateTime = Instant.now(); 
	}
	
	
	public static final TaskStatus accepted()
	{
	    return new TaskStatus(UUID.randomUUID(),
	        TaskStatusCode.ACCEPTED);
	}


	@Override
    public UUID getTaskID()
    {
        return taskID;
    }


    @Override
    public TaskStatusCode getStatusCode()
    {
        return statusCode;
    }


    @Override
    public ReasonCode getReasonCode()
    {
        return reasonCode;
    }


    @Override
    public Instant getUpdateTime()
    {
        return updateTime;
    }


    @Override
    public TimeExtent getEstimatedExecutionTime()
    {
        return estimatedExecTime;
    }


    @Override
    public Duration getEstimatedDuration()
    {
        return estimatedDuration;
    }


    public Collection<CommandAck> getCommandsStatus()
    {
        return commandsStatus;
    }


    @Override
    public String getMessage()
    {
        return message;
    }
}
