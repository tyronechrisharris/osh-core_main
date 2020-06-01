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
import org.sensorhub.api.procedure.ProcedureEvent;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;
import com.google.common.base.Strings;


/**
 * <p>
 * Event sent when a new FOI is being targeted by a procedure.
 * It is immutable and carries feature data by reference.
 * </p>
 *
 * @author Alex Robin
 * @since Apr 23, 2015
 */
public class FoiEvent extends ProcedureEvent
{

    /**
	 * Description of Feature of Interest related to this event (by reference)
	 */
	protected IGeoFeature foi;


	/**
	 * Unique ID of feature of interest related to this event
	 */
	protected String foiUID;


	/**
	 * Time at which the feature of interest started being observed.<br/>
	 * Use {@link Double#NaN} with a value for {@link #stopTime} to end the
	 * FoI observation period.<br/>
	 */
	protected Instant startTime;


	/**
	 * Time at which the feature of interest stopped being observed.<br/>
     * Use {@link Double#NaN} with a value for {@link #startTime} to start a
     * new observation period for the FoI
	 */
	protected Instant stopTime;


    /**
     * Creates a {@link Type#NEW_FOI} event with only the feature ID
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procedureID ID of producer that generated the event
     * @param foiUID Unique ID of feature of interest
     * @param startTime time at which observation of the FoI started
     */
	public FoiEvent(long timeStamp, ProcedureId procedureID, String foiUID, Instant startTime)
    {
        super(timeStamp, procedureID);

        Asserts.checkArgument(!Strings.isNullOrEmpty(foiUID), "FOI UID must be set");
        Asserts.checkNotNull(startTime, "startTime");

        this.foiUID = foiUID;
        this.startTime = startTime;
    }


	/**
	 * Creates a {@link Type#NEW_FOI} event with only the feature ID
	 * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param producer producer that generated the event
	 * @param foiUID Unique ID of feature of interest
     * @param startTime time at which observation of the FoI started
	 */
	public FoiEvent(long timeStamp, IDataProducer producer, String foiUID, Instant startTime)
	{
	    this(timeStamp,
	        producer.getProcedureID(),
	        foiUID,
	        startTime);
        this.source = producer;
	}


	/**
     * @deprecated Use {@link #FoiEvent(long, IDataProducer, String, Instant)
     */
    @Deprecated
    @SuppressWarnings("javadoc")
    public FoiEvent(long timeStamp, IDataProducer producer, String foiUID, double startTime)
    {
	    this(timeStamp, producer, foiUID, Instant.ofEpochMilli((long)(startTime*1000.0)));
    }


	/**
     * Creates a {@link Type#NEW_FOI} event with an attached feature object
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param producer producer that generated the event
     * @param foi feature object
	 * @param startTime time at which observation of the FoI started
     */
	public FoiEvent(long timeStamp, IDataProducer producer, IGeoFeature foi, Instant startTime)
    {
	    this(timeStamp,
	        producer.getProcedureID(),
	        foi.getUniqueIdentifier(),
	        startTime);
        this.source = producer;
        this.foi = foi;
    }


	/**
     * @deprecated Use {@link #FoiEvent(long, IDataProducer, AbstractFeature, Instant)
     */
    @Deprecated
    @SuppressWarnings("javadoc")
    public FoiEvent(long timeStamp, IDataProducer producer, IGeoFeature foi, double startTime)
    {
	    this(timeStamp, producer, foi, Instant.ofEpochMilli((long)(startTime*1000.0)));
    }


	public IGeoFeature getFoi()
    {
        return foi;
    }


    public String getFoiUID()
    {
        return foiUID;
    }


    public Instant getStartTime()
    {
        return startTime;
    }


    public Instant getStopTime()
    {
        return stopTime;
    }

}
