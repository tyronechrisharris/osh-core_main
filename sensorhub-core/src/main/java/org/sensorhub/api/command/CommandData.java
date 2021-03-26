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
import org.sensorhub.api.utils.OshAsserts;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
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
    protected long commandStreamID;
    protected String senderID;
    protected Instant issueTime;
    protected DataBlock params;
    
    
    protected CommandData()
    {
        // can only instantiate with builder
        this.issueTime = Instant.now();
    }
    
    
    public CommandData(long commandStreamID, DataBlock params)
    {
        this.commandStreamID = OshAsserts.checkValidInternalID(commandStreamID);
        this.params = Asserts.checkNotNull(params, DataBlock.class);
        this.issueTime = Instant.now();
    }


    @Override
    public long getCommandStreamID()
    {
        return commandStreamID;
    }


    @Override
    public String getSenderID()
    {
        return senderID;
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
    
    
    /*
     * Builder
     */
    public static class Builder extends CommandDataBuilder<Builder, CommandData>
    {
        public Builder()
        {
            this.instance = new CommandData();
        }
        
        public static Builder from(ICommandData base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class CommandDataBuilder<
            B extends CommandDataBuilder<B, T>,
            T extends CommandData>
        extends BaseBuilder<T>
    {       
        protected CommandDataBuilder()
        {
        }
        
        
        protected B copyFrom(ICommandData base)
        {
            instance.commandStreamID = base.getCommandStreamID();
            instance.senderID = base.getSenderID();
            instance.issueTime = base.getIssueTime();
            instance.params = base.getParams();
            return (B)this;
        }


        public B withCommandStream(long id)
        {
            instance.commandStreamID = id;
            return (B)this;
        }
        

        public B withIssueTime(Instant issueTime)
        {
            instance.issueTime = issueTime;
            return (B)this;
        }


        public B withParams(DataBlock params)
        {
            instance.params = params;
            return (B)this;
        }
        
        
        public T build()
        {
            OshAsserts.checkValidInternalID(instance.commandStreamID, "commandStreamID");
            Asserts.checkNotNull(instance.params, "params");
            return super.build();
        }
    }
}
