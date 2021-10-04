/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.command;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import org.vast.util.Asserts;


/**
 * <p>
 * Event carrying command acknowledgement data
 * </p>
 *
 * @author Alex Robin
 * @date Mar 9, 2021
 */
public class CommandAckEvent extends CommandStreamEvent
{
    protected Collection<ICommandAck> commandAcks;
    
    
    public CommandAckEvent(long timeStamp, String sysUID, String controlInputName, ICommandAck... commandAcks)
    {
        super(timeStamp, sysUID, controlInputName);
        this.commandAcks = Asserts.checkNotNullOrEmpty(Arrays.asList(commandAcks), ICommandAck[].class);
    }
    
    
    public static CommandAckEvent success(IStreamingControlInterface controlInterface, ICommandData command)
    {
        return success(controlInterface, command, Instant.now());
    }
    
    
    public static CommandAckEvent success(IStreamingControlInterface controlInterface, ICommandData command, Instant actuationTime)
    {
        return new CommandAckEvent(
            System.currentTimeMillis(),
            controlInterface.getParentProducer().getUniqueIdentifier(),
            controlInterface.getName(),
            CommandAck.success(command, actuationTime));
    }
    
    
    public static CommandAckEvent fail(IStreamingControlInterface controlInterface, ICommandData command)
    {
        return fail(controlInterface, command, null);
    }
    
    
    public static CommandAckEvent fail(IStreamingControlInterface controlInterface, ICommandData command, Exception error)
    {
        return new CommandAckEvent(
            System.currentTimeMillis(),
            controlInterface.getParentProducer().getUniqueIdentifier(),
            controlInterface.getName(),
            CommandAck.fail(command, error));
    }


    public Collection<ICommandAck> getCommandAcks()
    {
        return commandAcks;
    }

}
