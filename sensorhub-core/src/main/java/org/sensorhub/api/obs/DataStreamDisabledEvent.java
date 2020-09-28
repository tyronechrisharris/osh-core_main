/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.obs;

import org.sensorhub.api.procedure.ProcedureId;


/**
 * <p>
 * Event sent when a datastream (i.e. output) is disabled by a procedure
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2020
 */
public class DataStreamDisabledEvent extends DataStreamEvent
{
    
    /**
     * Pass-through to super class constructor
     * @see DataStreamEvent#DataStreamEvent(long, ProcedureId, String, long)
     */
    public DataStreamDisabledEvent(long timeStamp, ProcedureId procedureId, String dataStreamName, long dataStreamID)
    {
        super(timeStamp, procedureId, dataStreamName, dataStreamID);
    }
    
    
    /**
     * Pass-through to super class constructor
     * @see DataStreamEvent#DataStreamEvent(ProcedureId, String, long)
     */
    public DataStreamDisabledEvent(ProcedureId procedureId, String dataStreamName, long dataStreamID)
    {
        super(procedureId, dataStreamName, dataStreamID);
    }
}
