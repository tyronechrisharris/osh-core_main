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
    String outputName;


    /**
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procUID Unique ID of parent procedure
     * @param outputName Name of output producing the datastream
     */
    public DataStreamEvent(long timeStamp, String procUID, String outputName)
    {
        super(timeStamp, procUID);
        this.outputName = outputName;
    }
    
    
    /**
     * Helper constructor that sets the timestamp to current system time
     * @param procUID Unique ID of parent procedure
     * @param outputName Name of output producing the datastream
     */
    public DataStreamEvent(String procUID, String outputName)
    {
        super(procUID);
        this.outputName = outputName;
    }


    /**
     * @return Name of datastream
     */
    public String getOutputName()
    {
        return outputName;
    }
}
