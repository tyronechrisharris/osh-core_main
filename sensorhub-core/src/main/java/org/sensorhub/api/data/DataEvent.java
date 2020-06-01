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

import java.time.Instant;
import org.sensorhub.api.common.ProcedureId;
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
    protected String foiUID;
    protected Instant resultTime;
    protected String channelID;
    protected DataBlock[] records;


    /**
     * Constructs a data event associated to a specific procedure and channel
     * @param timeStamp Time of event generation (unix time in milliseconds, base 1970)
     * @param procedureID ID of procedure that produced the data records
     * @param channelID ID/name of the output interface that generated the data
     * @param records Array of data records that triggered this event
     */
    public DataEvent(long timeStamp, ProcedureId procedureID, String channelID, DataBlock ... records)
    {
        super(timeStamp, procedureID);

        Asserts.checkArgument(!Strings.isNullOrEmpty(channelID), "channelID must be set");
        Asserts.checkArgument(records != null && records.length > 0, "records must be provided");

        this.channelID = channelID;
        this.records = records;
    }


    /**
     * Helper constructor to construct a data event associated to the procedure
     * that is the parent of the given data interface
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param dataInterface stream interface that generated the associated data
     * @param records Array of records that triggered this event
     */
    public DataEvent(long timeStamp, IStreamingDataInterface dataInterface, DataBlock ... records)
    {
        this(timeStamp,
             dataInterface.getParentProducer().getProcedureID(),
             dataInterface.getName(),
             records);
        this.source = dataInterface;
    }


    /**
     * Constructs a data event associated to a specific procedure, channel and FOI
     * @param timeStamp Time of event generation (unix time in milliseconds, base 1970)
     * @param procedureID ID of procedure that produced the data records
     * @param channelID ID/name of the output interface that generated the data
     * @param foiUID Unique ID of feature of interest that this data applies to
     * @param records Array of data records that triggered this event
     */
    public DataEvent(long timeStamp, ProcedureId procedureID, String channelID, String foiUID, DataBlock ... records)
    {
        this(timeStamp, procedureID, channelID, records);
        this.foiUID = foiUID;
    }


    /**
     * Constructs a data event associated to a specific procedure and channel,
     * and tagged by a specific result time.
     * @param timeStamp Time of event generation (unix time in milliseconds, base 1970)
     * @param procedureID ID of procedure that produced the data records
     * @param channelID ID/name of the output interface that generated the data
     * @param resultTime Time at which the data was generated (e.g. model run time)
     * @param records Array of data records that triggered this event
     */
    public DataEvent(long timeStamp, ProcedureId procedureID, String channelID, Instant resultTime, DataBlock ... records)
    {
        this(timeStamp, procedureID, channelID, records);
        this.resultTime = resultTime;
    }


    /**
     * Constructs a data event associated to a specific procedure, channel and FOI,
     * and tagged by a specific result time.
     * @param timeStamp Time of event generation (unix time in milliseconds, base 1970)
     * @param procedureID ID of procedure that produced the data records
     * @param channelID ID/name of the output interface that generated the data
     * @param resultTime Time at which the data was generated (e.g. model run time)
     * @param foiUID Unique ID of feature of interest that this data applies to
     * @param records Array of data records that triggered this event
     */
    public DataEvent(long timeStamp, ProcedureId procedureID, String channelID, Instant resultTime, String foiUID, DataBlock ... records)
    {
        this(timeStamp, procedureID, channelID, records);
        this.resultTime = resultTime;
        this.foiUID = foiUID;
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
            sourceID = EventUtils.getProcedureOutputSourceID(procedureID.getUniqueID(), channelID);
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
     * @return The time at which the data records were generated by the procedure
     */
    public Instant getResultTime()
    {
        return resultTime;
    }


    /**
     * @return Unique ID of feature of interest that this data applies to
     */
    public String getFoiUID()
    {
        return foiUID;
    }


    /**
     * @return List of data records attached to this event.<br/>
     * Multiple records can be associated to a single event because with high
     * rate or batch producers (e.g. models), it is often not practical or a
     * waste of resources to generate an event for every single record of measurements.
     * Note that all records share the same procedure, foi and result time.
     */
    public DataBlock[] getRecords()
    {
        return records;
    }
}
