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


/**
 * <p>
 * Immutable class used as command ACK
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public class CommandAck implements ICommandAck
{
    protected long commandRefID;
    protected Instant actuationTime;
    protected CommandStatusCode statusCode;
    protected Exception error;
    
    
    protected CommandAck(long commandRefID, CommandStatusCode statusCode)
    {
        this(commandRefID, statusCode, Instant.now());
    }
    
    
    protected CommandAck(long commandRefID, CommandStatusCode statusCode, Instant actuationTime)
    {
        this.commandRefID = commandRefID;
        this.statusCode = statusCode;
        this.actuationTime = actuationTime;        
    }
    
    
    public static ICommandAck success(long commandRefID)
    {
        return new CommandAck(commandRefID, CommandStatusCode.SUCCESS);
    }
    
    
    public static ICommandAck success(long commandRefID, Instant actuationTime)
    {
        return new CommandAck(commandRefID, CommandStatusCode.SUCCESS, actuationTime);
    }
    
    
    public static ICommandAck fail(long commandRefID)
    {
        return new CommandAck(commandRefID, CommandStatusCode.FAILED);
    }
    
    
    public static ICommandAck fail(long commandRefID, Exception error)
    {
        var ack = new CommandAck(commandRefID, CommandStatusCode.FAILED);
        ack.error = error;
        return ack;
    }


    @Override
    public long getCommandRefID()
    {
        return commandRefID;
    }


    @Override
    public Instant getActuationTime()
    {
        return actuationTime;
    }


    @Override
    public CommandStatusCode getStatusCode()
    {
        return statusCode;
    }


    @Override
    public Exception getError()
    {
        return error;
    }
}
