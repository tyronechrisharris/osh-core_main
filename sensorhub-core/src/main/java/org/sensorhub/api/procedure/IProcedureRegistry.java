/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.IHistoricalObsDatabase;
import org.sensorhub.api.event.IEventSource;
import org.sensorhub.api.event.IEventSourceInfo;
import org.sensorhub.impl.event.EventSourceInfo;


/**
 * <p>
 * There is one procedure registry per sensor hub that is used to keep the list
 * and state of all procedures registered on this hub. (i.e. sensors, actuators,
 * processes, other data sources). The registry generates events when procedures
 * are added and removed from the registry. Implementations backed by a
 * persistent store can be used to persist procedures' state across restarts.
 * </p><p>
 * <i>Note that implementations of this interface may not expose the driver/module
 * objects directly. They usually keep a shadow object reflecting the state of
 * the procedure instead; thus clients of this interface should not rely on
 * specific functionality of the module itself (e.g. sensor driver).</i>
 * </p>
 *
 * @author Alex Robin
 * @since Feb 18, 2019
 * @see IProcedureWithState IProcedure
 */
public interface IProcedureRegistry extends IEventSource
{
    public static final String EVENT_SOURCE_ID = "urn:osh:procedures";
    public static final IEventSourceInfo EVENT_SOURCE_INFO = new EventSourceInfo(IProcedureRegistry.EVENT_SOURCE_ID);
    
    
    /**
     * Registers a procedure with this registry.
     * This adds the procedure to the underlying data store if not already there
     * and takes care of forwarding all events to the event bus.
     * <p><i>Procedures implemented as modules (typically sensor drivers) are 
     * automatically registered when they are loaded by the module registry
     * but all other procedure instances must be registered explicitly.</i></p>
     * @param proc The live procedure instance
     * @return The key assigned to the new procedure
     */
    public FeatureId register(IProcedureWithState proc);
    
    
    /**
     * Unregisters the procedure with the given unique ID.<br/>
     * Note that unregistering the procedure doesn't remove it from the
     * data store but only disconnects the live procedure.
     * @param proc The live procedure instance
     */
    public void unregister(IProcedureWithState proc);
    
    
    /**
     * Retrieves the shadow object for the procedure with the given unique ID
     * @param uid The procedure UID
     * @return The procedure shadow
     */
    public IProcedureWithState get(String uid);
    
    
    /**
     * @return The data store containing the latest state of all procedures
     * previously registered on this hub.
     */
    public IHistoricalObsDatabase getProcedureStateDatabase();


    /**
     * @return The event source information for this registry
     */
    public default IEventSourceInfo getEventSourceInfo()
    {
        return EVENT_SOURCE_INFO;
    }
    
    
    /**
     * @return The sensor hub this registry is attached to
     */
    public ISensorHub getParentHub();
    
}
