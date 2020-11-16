/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.sensorhub.api.event.IEventProducer;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Base interface for all OSH procedure drivers that allow communication
 * with a procedure and provide access to its current state (i.e. SensorML)
 * </p><p>
 * An instance of this class can represent a hardware device (e.g. sensors,
 * actuators) or logical procedures (e.g. digital process, method, survey).
 * </p>
 *
 * @author Alex Robin
 * @since June 9, 2017
 */
public interface IProcedureDriver extends IEventProducer
{

    /**
     * @return The procedure name
     */
    public String getName();
    
    
    /**
     * @return A short description of the procedure
     */
    public String getDescription();
    
    
    /**
     * @return The procedure's unique identifier
     */
    public String getUniqueIdentifier();


    /**
     * @return The unique ID of the parent procedure group or null if this
     * procedure is not a member of any group
     */
    public String getParentGroupUID();


    /**
     * @return The parent procedure group or null if this procedure is not
     * a member of any group
     */
    public IProcedureGroupDriver<? extends IProcedureDriver> getParentGroup();


    /**
     * Retrieves the most current SensorML description of the procedure.
     * All implementations must return an instance of AbstractProcess with
     * a valid unique identifier.<br/>
     * In the case of a module generating data from multiple procedures (e.g.
     * sensor network), this returns the description of the group as a whole.
     * @return The SensorML description of the procedure or null if none
     * is available at the time of the call
     */
    public AbstractProcess getCurrentDescription();


    /**
     * Used to check when the SensorML description was last updated.
     * This is useful to avoid requesting the object when it hasn't changed.
     * @return Date/time of last description update as unix time (millis since 1970)
     * or {@link Long#MIN_VALUE} if description was never updated.
     */
    public long getLastDescriptionUpdate();


    /**
     * Check if the procedure is enabled. A procedure marked as disabled
     * is not producing live data but historical data may still be available.
     * @return True if procedure is enabled, false otherwise
     */
    public boolean isEnabled();

}