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

import java.time.Instant;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Immutable class holding both command data and corresponding ACK once
 * received from the control interface.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 11, 2021
 */
public class CommandDataWithAck implements ICommandDataWithAck
{
    ICommandData command;
    ICommandAck ack;
    

    public CommandDataWithAck(ICommandData command, ICommandAck ack)
    {
        this.command = Asserts.checkNotNull(command, ICommandData.class);
        this.ack = Asserts.checkNotNull(ack, ICommandAck.class);
    }
    
    
    @Override
    public long getCommandStreamID()
    {
        return command.getCommandStreamID();
    }


    @Override
    public String getSenderID()
    {
        return command.getSenderID();
    }


    @Override
    public long getCommandRefID()
    {
        return command.getCommandRefID();
    }


    @Override
    public Instant getIssueTime()
    {
        return command.getIssueTime();
    }


    @Override
    public DataBlock getParams()
    {
        return command.getParams();
    }


    @Override
    public Instant getActuationTime()
    {
        return ack.getActuationTime();
    }


    @Override
    public CommandStatusCode getStatusCode()
    {
        return ack.getStatusCode();
    }


    @Override
    public Exception getError()
    {
        return ack.getError();
    }
}
