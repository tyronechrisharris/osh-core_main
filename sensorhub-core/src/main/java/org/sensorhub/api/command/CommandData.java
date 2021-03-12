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
 * Immutable object carrying data for a single command associated to a
 * specific command stream.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public class CommandData implements ICommandData
{
    protected ICommandStreamInfo commandStream;
    protected String senderID;
    protected long commandRefID = 0;
    protected Instant issueTime = null;
    protected DataBlock params;
    
    
    public CommandData(ICommandStreamInfo commandStream, long refID, DataBlock params)
    {
        this.commandStream = Asserts.checkNotNull(commandStream, ICommandStreamInfo.class);
        this.commandRefID = refID;
        this.params = Asserts.checkNotNull(params, DataBlock.class);
    }


    @Override
    public ICommandStreamInfo getCommandStream()
    {
        return commandStream;
    }


    @Override
    public String getSenderID()
    {
        return senderID;
    }


    @Override
    public long getCommandRefID()
    {
        return commandRefID;
    }


    @Override
    public Instant getIssueTime()
    {
        return issueTime;
    }


    @Override
    public DataBlock getParams()
    {
        return params;
    }


    @Override
    public Instant getActuationTime()
    {
        throw new IllegalStateException("Command not executed yet");
    }
}
