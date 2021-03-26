/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import java.time.Instant;
import org.sensorhub.api.command.ICommandDataWithAck;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * ICommandData delegate used to override behavior of an existing ICommandData
 * implementation. 
 * </p>
 *
 * @author Alex Robin
 * @date Mar 24, 2021
 */
public class CommandDelegate implements ICommandDataWithAck
{
    ICommandDataWithAck delegate;


    public CommandDelegate(ICommandDataWithAck cmd)
    {
        this.delegate = cmd;
    }
    
    
    public long getCommandStreamID()
    {
        return delegate.getCommandStreamID();
    }


    public long getCommandRefID()
    {
        return delegate.getCommandRefID();
    }


    @Override
    public String getSenderID()
    {
        return delegate.getSenderID();
    }


    public Instant getActuationTime()
    {
        return delegate.getActuationTime();
    }


    public Instant getIssueTime()
    {
        return delegate.getIssueTime();
    }


    @Override
    public DataBlock getParams()
    {
        return null;
    }


    @Override
    public CommandStatusCode getStatusCode()
    {
        return delegate.getStatusCode();
    }


    @Override
    public Exception getError()
    {
        return delegate.getError();
    }
}
