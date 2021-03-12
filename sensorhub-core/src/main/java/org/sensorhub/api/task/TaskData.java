/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.task;

import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import org.sensorhub.api.command.CommandData;
import org.sensorhub.api.procedure.ProcedureId;
import org.vast.util.BaseBuilder;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Immutable implementation of {@link ITask} used to create new tasks using
 * the provided builder.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public class TaskData implements ITask
{
    protected ProcedureId procedureID;
    protected String senderID;
    protected UUID taskID;
    protected int priority;
    protected Instant creationTime;
    protected TimeExtent requestedExecutionTime;
    protected Collection<CommandData> commands;
    protected boolean exclusiveControl;
    
    
    protected TaskData()
    {        
    }


    @Override
    public ProcedureId getProcedureID()
    {
        return procedureID;
    }


    @Override
    public String getSenderID()
    {
        return senderID;
    }


    @Override
    public UUID getTaskID()
    {
        return taskID;
    }


    @Override
    public int getPriority()
    {
        return priority;
    }


    public Instant getCreationTime()
    {
        return creationTime;
    }


    @Override
    public TimeExtent getRequestedExecutionTime()
    {
        return requestedExecutionTime;
    }


    @Override
    public boolean isExclusiveControl()
    {
        return exclusiveControl;
    }


    @Override
    public Collection<CommandData> getCommands()
    {
        return commands;
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends TaskDataBuilder<Builder, TaskData>
    {
        public Builder()
        {
            this.instance = new TaskData();
        }
        
        public static Builder from(ITask base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class TaskDataBuilder<
            B extends TaskDataBuilder<B, T>,
            T extends TaskData>
        extends BaseBuilder<T>
    {       
        protected TaskDataBuilder()
        {
        }
        
        
        protected B copyFrom(ITask base)
        {
            instance.procedureID = base.getProcedureID();
            instance.senderID = base.getSenderID();
            instance.taskID = base.getTaskID();
            instance.priority = base.getPriority();
            instance.creationTime = base.getCreationTime();
            instance.requestedExecutionTime = base.getRequestedExecutionTime();
            instance.commands = base.getCommands();
            instance.exclusiveControl = base.isExclusiveControl();
            return (B)this;
        }
        
        
        public B withProcedureID(ProcedureId procId)
        {
            instance.procedureID = procId;
            return (B)this;
        }
    }
}
