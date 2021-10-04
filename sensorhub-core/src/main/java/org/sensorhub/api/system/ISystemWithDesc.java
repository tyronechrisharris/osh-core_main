/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.system;

import org.vast.ogc.om.IProcedure;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Interface for observing system resources associated to a SensorML description.
 * </p><p>
 * An instance of this class can be used to model different kinds of systems,
 * and with different levels of granularity. Examples of systems this class
 * can represent are:
 * <li>a hardware device (sensors, actuators)</li>
 * <li>a complex system (e.g. a complete robot, a vehicle, a smartphone)</li>
 * <li>a logical system (e.g. an executable instance of a process or algorithm)</li>
 * <li>a human being or animal implementing a specific procedure</li>
 * </p><p>
 * Note that a system is an instance of a procedure implemented by the
 * IProcedure interface in OSH. For a hardware system, the procedure is typically
 * the system model as described by its datasheet. For humans, the procedure
 * is the list of steps that have to be carried to collect a sample and/or
 * make an observation.
 * </p>
 * 
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public interface ISystemWithDesc extends IProcedure
{

    /**
     * @return The system type as a URI
     */
    @Override
    public default String getType()
    {
        var sml = getFullDescription();
        return sml != null ? sml.getType() : null;
    }
    
    
    AbstractProcess getFullDescription();
    
}
