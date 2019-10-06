/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.sensor;

import org.sensorhub.api.common.CommandStatus;
import org.sensorhub.api.data.IStreamingControlInterface;
import org.sensorhub.api.procedure.ProcedureEvent;


/**
 * <p>
 * Special type of immutable event carrying status data by reference
 * </p>
 *
 * @author Alex Robin
 * @since Nov 5, 2010
 */
public class SensorControlEvent extends ProcedureEvent
{
	protected CommandStatus status;
	
	
	/**
     * Constructs the event for an individual sensor
     * @param timeStamp unix time of event generation
     * @param controlInterface sensor control interface that generated the event
     * @param status status of command at time the event is generated
     */
    public SensorControlEvent(long timeStamp, IStreamingControlInterface controlInterface, CommandStatus status)
    {
        this(timeStamp,
            controlInterface.getParentProducer().getUniqueIdentifier(),
            controlInterface,
            status);
    }
    
    
    /**
     * Constructs the event for a sensor that is part of a network
     * @param timeStamp unix time of event generation
     * @param sensorID Unique ID of individual sensor in the network
     * @param controlInterface sensor control interface that generated the event
     * @param status status of command at time the event is generated
     */
    public SensorControlEvent(long timeStamp, String sensorID, IStreamingControlInterface controlInterface, CommandStatus status)
    {
        super(timeStamp, controlInterface.getParentProducer().getUniqueIdentifier());        
        this.source = controlInterface;
        this.status = status;
    }
	

    /**
     * @return The command status when this event was generated
     */
    public CommandStatus getStatus()
    {
        return status;
    }
    
    
    @Override
    public IStreamingControlInterface getSource()
    {
        return (IStreamingControlInterface)this.source;
    }
}
