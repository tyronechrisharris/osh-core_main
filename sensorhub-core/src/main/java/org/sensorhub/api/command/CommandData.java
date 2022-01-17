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

import java.math.BigInteger;
import java.time.Instant;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.utils.ObjectUtils;
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
    public static final String UNKNOWN_SENDER = "%NA%";
    
    protected BigInteger id;
    protected long commandStreamID;
    protected long foiID;
    protected String senderID;
    protected Instant issueTime;
    protected DataBlock params;
    
    
    protected CommandData()
    {
        // can only instantiate with builder
    }
    
    
    /**
     * Helper constructor to send a command directly to a driver.
     * You need to use the builder when sending a command via the event system
     * since other parameters are needed in this case.
     * @param id Command ID
     * @param params Command parameters
     */
    public CommandData(long id, DataBlock params)
    {
        this.id = BigInteger.valueOf(id);
        this.commandStreamID = 1;
        this.params = Asserts.checkNotNull(params, DataBlock.class);
        this.issueTime = Instant.now();
    }
    
    
    @Override
    public BigInteger getID()
    {
        return id;
    }
    
    
    @Override
    public void assignID(BigInteger id)
    {
        Asserts.checkState(this.id == null, "Command ID cannot be reassigned");
        this.id = id;
    }


    @Override
    public long getCommandStreamID()
    {
        return commandStreamID;
    }


    @Override
    public long getFoiID()
    {
        return foiID;
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


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
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
            instance.id = base.getID();
            instance.commandStreamID = base.getCommandStreamID();
            instance.foiID = base.getFoiID();
            instance.senderID = base.getSenderID();
            instance.issueTime = base.getIssueTime();
            instance.params = base.getParams();
            return (B)this;
        }


        public B withId(BigInteger id)
        {
            instance.id = id;
            return (B)this;
        }


        public B withId(long id)
        {
            instance.id = BigInteger.valueOf(id);
            return (B)this;
        }


        public B withCommandStream(long id)
        {
            instance.commandStreamID = id;
            return (B)this;
        }


        public B withFoi(long id)
        {
            instance.foiID = id;
            return (B)this;
        }


        public B withSender(String id)
        {
            instance.senderID = id;
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
            if (instance.issueTime == null)
                instance.issueTime = Instant.now();
            return super.build();
        }
    }
}
