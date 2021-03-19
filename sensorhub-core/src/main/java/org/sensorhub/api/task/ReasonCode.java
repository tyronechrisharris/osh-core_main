/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.task;


/**
 * <p>
 * Task status reason code enumeration
 * </p>
 *
 * @author Alex Robin
 * @date Mar 19, 2021
 */
public enum ReasonCode
{
    /** System canceled or re-scheduled task because it was overriden by higher priority task **/
    HIGHER_PRIORITY_OVERRIDE,
    
    /** System cannot handle task because it is being shut down **/
    SHUTTING_DOWN,
    
    /** System cannot handle task because it is over capacity **/
    OVER_CAPACITY,
    
    /** Other reason. See status message for details **/
    OTHER,
}