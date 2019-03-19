/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.common;


/**
 * <p>
 * Event sent when a procedure is disabled
 * </p>
 *
 * @author Alex Robin
 * @date Mar 2, 2019
 */
public class ProcedureDisabledEvent extends ProcedureEvent
{
    
    /**
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param sourceID ID of the event source
     * @param procedureID ID of disabled procedure
     */
    public ProcedureDisabledEvent(long timeStamp, String sourceID, String procedureID)
    {
        super(timeStamp, sourceID, procedureID);
    }
}
