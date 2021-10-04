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
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.system.SystemId;
import org.vast.util.BaseBuilder;
import com.google.common.collect.ImmutableList;


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
    protected SystemId systemID;
    protected String senderID;
    protected UUID taskID;
    protected Instant creationTime;
    protected Collection<ICommandData> commands;
    protected boolean requestExclusiveControl;
    
    
    protected TaskData()
    {        
    }


    @Override
    public SystemId getSystemID()
    {
        return systemID;
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


    public Instant getCreationTime()
    {
        return creationTime;
    }


    @Override
    public boolean isRequestExclusiveControl()
    {
        return requestExclusiveControl;
    }


    @Override
    public Collection<ICommandData> getCommands()
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
            instance.systemID = base.getSystemID();
            instance.senderID = base.getSenderID();
            instance.taskID = base.getTaskID();
            instance.creationTime = base.getCreationTime();
            instance.commands = base.getCommands();
            instance.requestExclusiveControl = base.isRequestExclusiveControl();
            return (B)this;
        }
        
        
        public B withSystemID(SystemId procId)
        {
            instance.systemID = procId;
            return (B)this;
        }
        
        
        public B withSenderID(String senderID)
        {
            instance.senderID = senderID;
            return (B)this;
        }
        
        
        public B withTaskID(UUID taskID)
        {
            instance.taskID = taskID;
            return (B)this;
        }
        
        
        public B withCreationTime(Instant creationTime)
        {
            instance.creationTime = creationTime;
            return (B)this;
        }
        
        
        public B withCommands(ICommandData commands)
        {
            return withCommands(Arrays.asList(commands));
        }


        public B withCommands(Collection<ICommandData> commands)
        {
            instance.commands = ImmutableList.copyOf(commands);
            return (B)this;
        }
        
        
        @Override
        public T build()
        {
            if (instance.creationTime == null)
                instance.creationTime = Instant.now();
            return super.build();
        }
    }
}
