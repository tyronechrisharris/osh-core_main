/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.command;

import java.util.concurrent.CompletableFuture;
import org.sensorhub.api.event.IEventProducer;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Interface for all taskable components using SWE Common model to describe
 * structure and encoding of commands they accept (e.g. actuators, processes...)
 * </p>
 *
 * @author Alex Robin
 * @since Mar 23, 2017
 */
public interface IStreamingControlInterface extends IEventProducer
{

    /**
     * Allows by-reference access to parent module
     * @return parent module instance
     */
    public ICommandReceiver getParentProducer();
    
    
    /**
     * Gets the interface name.
     * @return name of this control interface
     */
    public String getName();


    /**
     * Checks if this interface is enabled
     * @return true if interface is enabled, false otherwise
     */
    public boolean isEnabled();
    
    
    /**
     * Retrieves description of command message
     * Note that this can be a choice of multiple messages
     * @return Data component containing message structure
     */
    public DataComponent getCommandDescription();
    
    
    /**
     * Executes the provided command, potentially asynchronously.
     * @param command Command data
     * @return A future that will be completed normally when the command is accepted
     * and the callee is ready to receive the next command. Note that the command
     * may not have finished executing at this point. Command success/failure is notified
     * separately using a {@link CommandAckEvent} sent to all registered listeners.)<br/>
     * If an error can be detected early, the future will complete exceptionally with a 
     * {@link CompletionException}
     */
    public CompletableFuture<Void> executeCommand(ICommandData command);
    
    
    /**
     * Validates the command parameters without executing the command. This
     * is used for validating a task before it can be accepted for execution. 
     * @param command
     * @throws CommandException if the command is invalid
     */
    public void validateCommand(DataBlock command) throws CommandException;
}
