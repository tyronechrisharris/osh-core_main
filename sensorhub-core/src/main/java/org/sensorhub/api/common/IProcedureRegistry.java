/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.common;

import java.lang.ref.WeakReference;

/**
 * <p>
 * There is one procedure registry per sensor hub that is used to keep the list
 * and state of all registered procedures (i.e. sensors, actuators, processes,
 * other data sources) on this hub. It also generates events when procedures are
 * added and removed from the registry, and usually provide ways to persist
 * this state across restarts.
 * </p>
 * <p>
 * <i>Note that implementations of this interface may not expose the driver/module
 * objects directly. They usually keep a shadow object reflecting the state of
 * the procedure instead; thus clients of this interface should to rely on
 * specific functionality of the module itself (e.g. sensor driver).</i>
 * </p>
 *
 * @author Alex Robin
 * @since Feb 18, 2019
 * @see IProcedure IProcedure
 */
public interface IProcedureRegistry
{
    public static final String EVENT_SOURCE_ID = "urn:osh:procedurereg";
    
    
    /**
     * Registers a procedure with this registry.
     * <p><i>Procedures implemented as modules (typically sensor drivers) are automatically
     * registered when they are loaded by the module registry but all other
     * procedure instances must be registered explicitly.</i></p>
     * @param procedure
     */
    public void register(IProcedure procedure);
    
    
    /**
     * Unregisters the procedure with the given unique ID
     * @param uid procedure unique ID
     */
    public void unregister(String uid);
    
    
    /**
     * Retrieves a procedure using its unique ID.<br/>
     * Procedure group members can be retrieved directly using this method.
     * @param uid procedure unique ID
     * @return procedure with the given unique ID
     */
    public <T extends IProcedure> T get(String uid);
    
    
    /**
     * Retrieves the weak reference to a procedure using its unique ID.<br/>
     * Procedure group members can be retrieved directly using this method.
     * @param uid procedure unique ID
     * @return procedure with the given unique ID
     */
    public <T extends IProcedure> WeakReference<T> getRef(String uid);
    
    
    /**
     * Checks if registry contains the procedure with the given unique ID
     * @param uid procedure unique ID
     * @return true if a procedure with the given ID has been registered,
     * false otherwise
     */
    public boolean contains(String uid);
    
    
    /**
     * Retrieves all registered procedures of the given type.<br/>
     * Note that this method only returns top level procedures. Group members can be
     * retrieved by calling the group getMembers() method.
     * @param procedureType Type of procedures to return (e.g. ISensor, etc.)
     * @return Iterable collection of selected procedures
     */
    public <T extends IProcedure> Iterable<T> list(Class<T> procedureType);
    
}
