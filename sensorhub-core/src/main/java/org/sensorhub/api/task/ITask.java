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
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.system.SystemId;


/**
 * <p>
 * A task includes one or more commands that will be processed in sequence
 * or in parallel by the receiving system.
 * </p><p>
 * The command receiver typically does its best to reject the entire task if
 * one of the commands is invalid or cannot be executed, or if the particular
 * sequence of commands is not supported. If the tasking manager cannot detect
 * error conditions early, task execution will stop as soon as one of the
 * command fails.
 * </p><p>
 * A task can also be used to reserve a slot for exclusive access to one or
 * more control channels. Such a task does not include any embedded commands
 * but must set the boolean flag requestExclusiveControl.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public interface ITask
{
    SystemId getSystemID();
        
    String getSenderID();
    
    UUID getTaskID();    
    
    Instant getCreationTime();
    
    Collection<ICommandData> getCommands();

    boolean isRequestExclusiveControl();
}
