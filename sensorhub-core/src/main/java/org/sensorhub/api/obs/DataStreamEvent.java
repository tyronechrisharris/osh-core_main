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

import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.vast.util.Asserts;


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
    protected String outputName;


    /**
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procUID Unique ID of parent procedure
     * @param outputName Name of output producing the datastream
     */
    public DataStreamEvent(long timeStamp, String procUID, String outputName)
    {
        super(timeStamp, procUID);
        this.outputName = Asserts.checkNotNullOrEmpty(outputName, "outputName");
    }
    
    
    /**
     * Helper constructor that sets the timestamp to current system time
     * @param procUID Unique ID of parent procedure
     * @param outputName Name of output producing the datastream
     */
    public DataStreamEvent(String procUID, String outputName)
    {
        this(System.currentTimeMillis(), procUID, outputName);
    }
    
    
    /**
     * Helper constructor that sets the timestamp to current system time
     * @param dsInfo Data stream the event relates to
     */
    public DataStreamEvent(IDataStreamInfo dsInfo)
    {
        super(Asserts.checkNotNull(dsInfo, IDataStreamInfo.class).getProcedureID().getUniqueID());
        this.outputName = Asserts.checkNotNullOrEmpty(dsInfo.getOutputName(), "outputName");
    }


    /**
     * @return Name of datastream
     */
    public String getOutputName()
    {
        return outputName;
    }


    @Override
    public String getSourceID()
    {
        if (sourceID == null)
            sourceID = EventUtils.getDataStreamStatusTopicID(procedureUID, outputName);
        return sourceID;
    }
    
}
