/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.data;

import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.vast.util.Asserts;
import com.google.common.base.Strings;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Type of event generated when new data is available from a data producer.
 * It is immutable and carries data by reference.
 * </p>
 *
 * @author Alex Robin
 * @since Feb 20, 2015
 */
public class DataEvent extends ProcedureEvent
{
    protected String channelID;
    
    
	/**
	 * New data that triggered this event.<br/>
	 * Multiple records can be associated to a single event because with high
	 * rate producers, it is often not practical to generate an event for
	 * every single record of measurements.
	 */
	protected DataBlock[] records;
    
    
    /**
     * Constructs a data event associated to a specific procedure and channel
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procedureUID Unique ID of procedure that produced the data records
     * @param channelID ID/name of the output interface that generated the data
     * @param records arrays of records that triggered this notification
     */
    public DataEvent(long timeStamp, String procedureUID, String channelID, DataBlock ... records)
    {
        super(timeStamp, procedureUID);
        
        Asserts.checkArgument(!Strings.isNullOrEmpty(channelID), "channelID must be set");
        Asserts.checkArgument(records != null && records.length > 0, "records must be provided");
        
        this.channelID = channelID;
        this.records = records;
    }
	
	
	/**
	 * Constructs a data event associated to the procedure that is the parent 
	 * of the given data interface
	 * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param dataInterface stream interface that generated the associated data
	 * @param records arrays of records that triggered this notification
	 */
	public DataEvent(long timeStamp, IStreamingDataInterface dataInterface, DataBlock ... records)
	{
	    this(timeStamp,
	         dataInterface.getParentProducer().getUniqueIdentifier(),
	         dataInterface.getName(),
	         records);
        this.source = dataInterface;
	}
	
	
    @Override
    public IStreamingDataInterface getSource()
    {
        return (IStreamingDataInterface)this.source;
    }


    @Override
    public String getSourceID()
    {
        if (sourceID == null)
            sourceID = EventUtils.getProcedureOutputSourceID(procedureUID, channelID);
        return sourceID;
    }
    
    
    /**
     * @return Name of channel the event was produced on (e.g. output name)
     */
    public String getChannelID()
    {
        return channelID;
    }


    /**
     * @return list of data records produced
     */
    public DataBlock[] getRecords()
    {
        return records;
    }
}
