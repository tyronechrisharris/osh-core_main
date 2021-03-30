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
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataBlock;


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
    protected ICommandData command;
    protected Instant actuationTime;
    protected CommandStatusCode statusCode;
    protected Exception error;
    
    
    protected CommandAck(ICommandData cmd, CommandStatusCode statusCode, Instant actuationTime)
    {
        this.command = Asserts.checkNotNull(cmd, ICommandData.class);
        this.statusCode = Asserts.checkNotNull(statusCode, CommandStatusCode.class);
        this.actuationTime = actuationTime;        
    }
    
    
    public static ICommandAck success(ICommandData cmd)
    {
        return new CommandAck(cmd, CommandStatusCode.SUCCESS, Instant.now());
    }
    
    
    public static ICommandAck success(ICommandData cmd, Instant actuationTime)
    {
        Asserts.checkNotNull(actuationTime, "actuationTime");
        return new CommandAck(cmd, CommandStatusCode.SUCCESS, actuationTime);
    }
    
    
    public static ICommandAck fail(ICommandData cmd)
    {
        return new CommandAck(cmd, CommandStatusCode.FAILED, null);
    }
    
    
    public static ICommandAck fail(ICommandData cmd, Exception error)
    {
        var ack = new CommandAck(cmd, CommandStatusCode.FAILED, null);
        ack.error = error;
        return ack;
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


    public long getCommandStreamID()
    {
        return command.getCommandStreamID();
    }


    public String getSenderID()
    {
        return command.getSenderID();
    }


    public Instant getIssueTime()
    {
        return command.getIssueTime();
    }


    public DataBlock getParams()
    {
        return command.getParams();
    }
}
