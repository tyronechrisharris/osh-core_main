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

import java.lang.ref.WeakReference;
import java.util.concurrent.CompletableFuture;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.command.IStreamingControlInterface;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.database.IProcedureStateDatabase;
import org.vast.ogc.gml.IFeature;


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
 * specific functionality of the driver module itself (e.g. sensor driver).</i>
 * </p>
 *
 * @author Alex Robin
 * @since Feb 18, 2019
 * @see IProcedureDriver IProcedure
 */
public interface IProcedureRegistry
{
    
    /**
     * Asynchronously registers a procedure driver (e.g. sensor driver, etc.)
     * with this registry. Implementation of this method must take take care of
     * forwarding all events produced by the driver to the event bus.
     * <br/><br/>
     * If the procedure is a {@link IDataProducer}, this method takes care of
     * registering all FOIs associated with the procedure at the time of registration
     * (i.e. returned by {@link IDataProducer#getCurrentFeaturesOfInterest()}).
     * <br/><br/>
     * If the procedure is a {@link IProcedureGroupDriver}, this method takes
     * care of registering all group members defined by the driver at the time of
     * registration (i.e. returned by {@link IProcedureGroupDriver#getMembers()}).
     * <br/><br/>
     * Note that a single {@link ProcedureAddedEvent} is generated for the parent
     * procedure. No event is generated for members of a procedure group. 
     * @param proc The live procedure instance
     * @return A future that will be completed when the procedure is successfully
     * registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(IProcedureDriver proc);
    
    
    /**
     * Asynchronously registers a datastream / data interface.
     * @param dataStream The streaming data interface of a live procedure instance
     * @return A future that will be completed when the datastream is successfully
     * registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(IStreamingDataInterface dataStream);
    
    
    /**
     * Asynchronously registers a control interface.
     * @param controlStream The streaming control interface of a live procedure instance
     * @return A future that will be completed when the control interface is successfully
     * registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(IStreamingControlInterface controlStream);
    
    
    /**
     * Asynchronously registers a feature of interest.
     * @param proc The procedure observing the feature of interest
     * @param foi The feature of interest
     * @return A future that will be completed when the feature of interest is
     * successfully registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(IProcedureDriver proc, IFeature foi);


    /**
     * Unregisters a procedure and all its datastreams and features of interest.
     * If it is a procedure group, all of its members are also unregistered
     * recursively.
     * <br/><br/>
     * Note that unregistering the procedure doesn't remove it from the
     * data store but only disconnects the live procedure(s) and send a
     * {@link ProcedureDisabledEvent} event.
     * @param proc The live procedure instance
     * @return A future that will be completed when the control interface is
     * successfully unregistered or report an exception if an error occurred.
     */
    public CompletableFuture<Void> unregister(IProcedureDriver proc);
    
    
    /**
     * Checks if the given procedure is registered on this hub
     * @param uid
     * @return True if a driver has previously registered a procedure with the
     * given UID, false otherwise
     */
    public boolean isRegistered(String uid);
    
    
    /**
     * Retrieves a handle to the procedure driver with the given unique ID.
     * @param uid The procedure unique ID
     * @return A weak reference to the procedure driver instance, or a proxy if the
     * driver is not directly accessible
     */
    public <T extends IProcedureDriver> WeakReference<T> getProcedure(String uid);
    
    
    /**
     * @return The database containing the latest state of procedures registered
     * on this hub when they are not associated to a historical database.
     * Note that information contained in this database is also accessible as
     * read-only through the federated hub database.
     */
    public IProcedureStateDatabase getProcedureStateDatabase();


    /**
     * @return The sensor hub this registry is attached to
     */
    public ISensorHub getParentHub();

}
