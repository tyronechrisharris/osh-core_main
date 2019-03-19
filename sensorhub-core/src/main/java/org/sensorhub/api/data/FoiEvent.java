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

import org.sensorhub.api.common.ProcedureEvent;
import net.opengis.gml.v32.AbstractFeature;


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
	protected AbstractFeature foi;
	
	
	/**
	 * ID of feature of interest related to this event
	 */
	protected String foiID;
	
	
	/**
	 * Time at which the feature of interest started being observed.<br/>
	 * Use {@link Double#NaN} with a value for {@link #stopTime} to end the
	 * FoI observation period.<br/>
	 */
	protected double startTime;
	
	
	/**
	 * Time at which the feature of interest stopped being observed.<br/>
     * Use {@link Double#NaN} with a value for {@link #startTime} to start a
     * new observation period for the FoI 
	 */
	protected double stopTime;
    
    
    /**
     * Creates a {@link Type#NEW_FOI} event with only the feature ID
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param procedureID ID of producer that generated the event
     * @param sourceID Complete ID of event source
     * @param foiID ID of feature of interest
     * @param startTime time at which observation of the FoI started (unix time in seconds, base 1970)
     */
    public FoiEvent(long timeStamp, String procedureID, String sourceID, String foiID, double startTime)
    {
        super(timeStamp, procedureID, sourceID);
        this.foiID = foiID;
        this.startTime = startTime;
    }
	
	
	/**
	 * Creates a {@link Type#NEW_FOI} event with only the feature ID
	 * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param producer producer that generated the event
	 * @param foiID ID of feature of interest
     * @param startTime time at which observation of the FoI started (unix time in seconds, base 1970)
	 */
	public FoiEvent(long timeStamp, IDataProducer producer, String foiID, double startTime)
	{
	    this(timeStamp,
	        producer.getUniqueIdentifier(),
	        producer.getEventSourceInfo().getSourceID(),
	        foiID,
	        startTime);
        this.source = producer;
	}
	
	
	/**
     * Creates a {@link Type#NEW_FOI} event with an attached feature object
     * @param timeStamp time of event generation (unix time in milliseconds, base 1970)
     * @param producer producer that generated the event
     * @param foi feature object
	 * @param startTime time at which observation of the FoI started (unix time in seconds, base 1970)
     */
	public FoiEvent(long timeStamp, IDataProducer producer, AbstractFeature foi, double startTime)
    {
	    this(timeStamp,
	        producer.getUniqueIdentifier(),
            producer.getEventSourceInfo().getSourceID(),
	        foi.getUniqueIdentifier(),
	        startTime);        
        this.source = producer;
        this.foi = foi;
    }
	
	
	public AbstractFeature getFoi()
    {
        return foi;
    }


    public String getFoiID()
    {
        return foiID;
    }


    public double getStartTime()
    {
        return startTime;
    }


    public double getStopTime()
    {
        return stopTime;
    }

}
