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

import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureId;


/**
 * <p>
 * Base class for all datastream (i.e. output) related events
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2020
 */
public abstract class DataStreamEvent extends ProcedureEvent
{
    String dataStreamName;
    long dataStreamID;


    /**
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procedureId ID of parent procedure
     * @param dataStreamName Name of datastream (same as output name)
     * @param dataStreamID Internal ID of datastream
     */
    public DataStreamEvent(long timeStamp, ProcedureId procedureId, String dataStreamName, long dataStreamID)
    {
        super(timeStamp, procedureId);
        this.dataStreamName = dataStreamName;
        this.dataStreamID = dataStreamID;
    }
    
    
    /**
     * Helper constructor that sets the timestamp to current system time
     * @param procedureId ID of parent procedure
     * @param dataStreamName Name of datastream
     * @param dataStreamID Internal ID of datastream
     */
    public DataStreamEvent(ProcedureId procedureId, String dataStreamName, long dataStreamID)
    {
        super(procedureId);
        this.dataStreamName = dataStreamName;
        this.dataStreamID = dataStreamID;
    }


    /**
     * @return Name of datastream
     */
    public String getDataStreamName()
    {
        return dataStreamName;
    }


    /**
     * @return Internal ID of datastream
     */
    public long getDataStreamID()
    {
        return dataStreamID;
    }
}
