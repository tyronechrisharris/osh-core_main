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
    protected IGeoFeature foi;
	protected String foiUID;
	protected Instant startTime;
	//protected Instant stopTime;


    /**
     * Creates a {@link Type#NEW_FOI} event with only the feature ID
     * @param timeStamp Time of event generation (unix time in milliseconds, base 1970)
     * @param procUID Unique ID of producer that generated the event
     * @param foiUID Unique ID of feature of interest
     * @param startTime Time at which observation of the FoI started
     */
	public FoiEvent(long timeStamp, String procUID, String foiUID, Instant startTime)
    {
        super(timeStamp, procUID);

        Asserts.checkArgument(!Strings.isNullOrEmpty(foiUID), "FOI UID must be set");
        Asserts.checkNotNull(startTime, "startTime");

        this.foiUID = foiUID;
        this.startTime = startTime;
    }


	/**
	 * Creates a {@link Type#NEW_FOI} event with only the feature ID
	 * @param timeStamp Time of event generation (unix time in milliseconds, base 1970)
     * @param producer Producer that generated the event
	 * @param foiUID Unique ID of feature of interest
     * @param startTime Time at which observation of the FoI started
	 */
	public FoiEvent(long timeStamp, IDataProducer producer, String foiUID, Instant startTime)
	{
	    this(timeStamp,
	        producer.getUniqueIdentifier(),
	        foiUID,
	        startTime);
        this.source = producer;
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
	        producer.getUniqueIdentifier(),
	        foi.getUniqueIdentifier(),
	        startTime);
        this.source = producer;
        this.foi = foi;
    }


    /**
     * @return The unique ID of the feature of interest related to this event
     */
    public String getFoiUID()
    {
        return foiUID;
    }
    

	/**
     * @return The description of the feature of interest related to this event
     * or null if the description was already registered by some other means.
     * (in this case, only the UID is provided in the event)
     */
	public IGeoFeature getFoi()
    {
        return foi;
    }


	/**
     * @return The time at which the feature of interest started being observed.
     */
    public Instant getStartTime()
    {
        return startTime;
    }


    /*
     * @return The time at which the feature of interest stopped being observed.
    public Instant getStopTime()
    {
        return stopTime;
    }*/

}
