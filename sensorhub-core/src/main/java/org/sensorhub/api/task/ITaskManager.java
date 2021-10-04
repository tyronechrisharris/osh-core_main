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

import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * <p>
 * Interface to be implemented by drivers that need to implement specific 
 * task management logic (i.e. sequencing commands, scheduling, priority, etc.).
 * When a system driver implements task management, the hub's task manager
 * delegates all task management to it.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 11, 2021
 */
public interface ITaskManager
{
    
    /**
     * @return True if all commands must be sent via creation of a task on this
     * interface, false if commands can be sent directly to the control interfaces
     */
    public boolean isTaskRequired();
    
    
    /**
     * Retrieves the list of all tasks currently being processed by this system
     * @return Read-only map of task IDs to task objects
     */
    public Map<Long, ? extends ITask> getCurrentTasks();
    
    
    /**
     * Validates a new task asynchronously and sends an initial task status
     * @param task
     * @return Future that wil be completed when the initial task status is known. 
     */
    public default CompletableFuture<ITaskStatus> acceptTask(ITask task)
    {
        return CompletableFuture.completedFuture(TaskStatus.accepted());
    }
}
