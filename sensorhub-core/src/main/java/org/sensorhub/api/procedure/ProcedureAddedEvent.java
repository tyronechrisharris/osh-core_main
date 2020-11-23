/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

/**
 * <p>
 * Event sent when a procedure is added to the hub or to a procedure group
 * </p>
 *
 * @author Alex Robin
 * @date Mar 2, 2019
 */
public class ProcedureAddedEvent extends ProcedureEvent
{
    String parentGroupUID;


    /**
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procUID Unique ID of added procedure
     * @param parentGroupUID ID of parent procedure group (or null if procedure
     * is not a member of any group)
     */
    public ProcedureAddedEvent(long timeStamp, String procUID, String parentGroupUID)
    {
        super(timeStamp, procUID);
        this.parentGroupUID = parentGroupUID;
    }
    
    
    /**
     * Helper constructor that sets the timestamp to current system time
     */
    @SuppressWarnings("javadoc")
    public ProcedureAddedEvent(String procUID, String parentGroupUID)
    {
        super(procUID);
        this.parentGroupUID = parentGroupUID;
    }


    /**
     * @return Unique ID of parent procedure group
     */
    public String getParentGroupUID()
    {
        return parentGroupUID;
    }
}
