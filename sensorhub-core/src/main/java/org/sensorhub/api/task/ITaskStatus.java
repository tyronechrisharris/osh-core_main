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

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.vast.util.TimeExtent;


public interface ITaskStatus
{
    
    /**
     * @return ID of task that this status relates to, as assigned by task manager
     */
    UUID getTaskID();
    
    
    /**
     * @return Task status code
     */
    TaskStatusCode getStatusCode();
    
    
    /**
     * @return Task status reason code, or null if no reason was specified
     */
    ReasonCode getReasonCode();
    
    
    /**
     * @return Time at which this status was actual 
     */
    Instant getUpdateTime();
    
    
    /**
     * @return Estimated execution time range
     */
    TimeExtent getEstimatedExecutionTime();
    
    
    /**
     * @return Estimated execution duration
     */
    Duration getEstimatedDuration();
    
    
    /**
     * @return Additional status message
     */    
    String getMessage();
}
