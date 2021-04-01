/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.command;

import java.util.Arrays;
import java.util.Collection;
import org.vast.util.Asserts;


/**
 * <p>
 * Event carrying command data sent to a command receiver
 * </p>
 *
 * @author Alex Robin
 * @date Mar 9, 2021
 */
public class CommandEvent extends CommandStreamEvent
{
    protected Collection<ICommandData> commands;
    
    
    public CommandEvent(long timeStamp, String procUID, String controlInputName, ICommandData... commands)
    {
        super(timeStamp, procUID, controlInputName);
        this.commands = Asserts.checkNotNullOrEmpty(Arrays.asList(commands), ICommandData[].class);
        this.sourceID = Asserts.checkNotNullOrEmpty(commands[0].getSenderID(), "senderID");
    }
    
    
    public CommandEvent(String procUID, String controlInputName, ICommandData... commands)
    {
        this(System.currentTimeMillis(), procUID, controlInputName, commands);
    }


    public Collection<ICommandData> getCommands()
    {
        return commands;
    }
}
