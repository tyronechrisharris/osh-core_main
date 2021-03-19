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


/**
 * Task status code enumeration
 */
public enum TaskStatusCode
{
    /** Task was received but not yet evaluated **/
    PENDING,
    
    /** Task is valid and was accepted **/
    ACCEPTED,

    /** Task was rejected because it was invalid or cannot execute for other reasons.
        See errorCode and message for details **/
    REJECTED,
    
    /** Task was accepted and is scheduled to execute at a later time. **/
    SCHEDULED,
    
    /** Task execution has started and the status message contains a progress report **/
    INPROGRESS,
    
    /** Task execution is complete **/
    COMPLETED,
    
    /** A failure occured during the task execution.
        See errorCode and message for details **/
    FAILED,
    
    /** Task was cancelled before it terminated execution **/
    CANCELLED_BY_SENDER,
    CANCELLED_BY_RECEIVER
}