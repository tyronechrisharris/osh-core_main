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

import java.util.concurrent.CompletableFuture;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.command.IStreamingControlInterface;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.database.ISystemStateDatabase;
import org.vast.ogc.gml.IFeature;


/**
 * <p>
 * There is one system registry per sensor hub that is used to keep the list
 * and state of all systems registered on this hub. (i.e. sensors, actuators,
 * processes, other data sources). The registry generates events when systems
 * are added and removed from the registry. Implementations backed by a
 * persistent store can be used to persist systems state across restarts.
 * </p><p>
 * <i>Note that implementations of this interface may not expose the driver/module
 * objects directly. They usually keep a shadow object reflecting the state of
 * the system instead; thus clients of this interface should not rely on
 * specific functionality of the driver module itself.</i>
 * </p>
 *
 * @author Alex Robin
 * @since Feb 18, 2019
 * @see ISystemDriver
 */
public interface ISystemDriverRegistry
{
    
    /**
     * Asynchronously registers a system driver (e.g. sensor driver, etc.)
     * with this registry. Implementation of this method must take take care of
     * forwarding all events produced by the driver to the event bus.
     * <br/><br/>
     * If the system is a {@link IDataProducer}, this method takes care of
     * registering all FOIs associated with the system at the time of registration
     * (i.e. returned by {@link IDataProducer#getCurrentFeaturesOfInterest()}).
     * <br/><br/>
     * If the system is a {@link ISystemGroupDriver}, this method takes
     * care of registering all subsystems defined by the driver at the time of
     * registration (i.e. returned by {@link ISystemGroupDriver#getMembers()}).
     * <br/><br/>
     * Note that a single {@link SystemAddedEvent} is generated for the parent
     * system. No event is generated for members of a system group. 
     * @param proc The system driver instance
     * @return A future that will be completed when the system is successfully
     * registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(ISystemDriver proc);
    
    
    /**
     * Asynchronously registers a datastream / data interface.
     * @param dataStream The streaming data interface of a system driver
     * @return A future that will be completed when the datastream is successfully
     * registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(IStreamingDataInterface dataStream);
    
    
    /**
     * Asynchronously registers a control interface.
     * @param controlStream The streaming control interface of a system driver
     * @return A future that will be completed when the control interface is successfully
     * registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(IStreamingControlInterface controlStream);
    
    
    /**
     * Asynchronously registers a feature of interest.
     * @param proc The system observing or targeting the feature of interest
     * @param foi The feature of interest
     * @return A future that will be completed when the feature of interest is
     * successfully registered or report an exception if an error occurred.
     */
    public CompletableFuture<Boolean> register(ISystemDriver proc, IFeature foi);


    /**
     * Unregisters a system driver and all its datastreams, command streams
     * and subsystems, recursively.
     * <br/><br/>
     * Note that unregistering the system doesn't remove it from the
     * data store but only disconnects the system driver and send a
     * {@link SystemDisabledEvent} event.
     * @param proc The system driver instance
     * @return A future that will be completed when the control interface is
     * successfully unregistered or report an exception if an error occurred.
     */
    public CompletableFuture<Void> unregister(ISystemDriver proc);
    
    
    /**
     * Checks if the given system driver is registered on this hub
     * @param uid
     * @return True if a driver has previously registered a system with the
     * given UID, false otherwise
     */
    public boolean isRegistered(String uid);
    
    
    /**
     * @return The database containing the latest state of systems registered
     * on this hub when they are not associated to a historical database.
     * Note that information contained in this database is also accessible as
     * read-only through the federated hub database.
     */
    public ISystemStateDatabase getSystemStateDatabase();


    /**
     * @return The sensor hub this registry is attached to
     */
    public ISensorHub getParentHub();

}
