/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.command;

import org.sensorhub.api.common.BigId;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Command status report with a result
 * </p>
 *
 * @author Alex Robin
 * @since Sep 12, 2022
 */
public class CommandStatusWithResult extends CommandStatus
{
    ICommandResult result;
    
    
    protected CommandStatusWithResult()
    {
        // can only instantiate with builder
    }
    
    
    protected CommandStatusWithResult(BigId commandID, CommandStatusCode statusCode, TimeExtent execTime, ICommandResult result)
    {
        super(commandID, statusCode, execTime);
        this.result = Asserts.checkNotNull(result, ICommandResult.class);
    }
    

    @Override
    public ICommandResult getResult()
    {
        return result;
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends CommandStatusWithResultBuilder<Builder, CommandStatusWithResult>
    {
        public Builder()
        {
            this.instance = new CommandStatusWithResult();
        }
        
        public static Builder from(ICommandStatus base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class CommandStatusWithResultBuilder<
            B extends CommandStatusWithResultBuilder<B, T>,
            T extends CommandStatusWithResult>
        extends CommandStatusBuilder<B, T>
    {       
        protected CommandStatusWithResultBuilder()
        {
        }
        
        
        @Override
        protected B copyFrom(ICommandStatus base)
        {
            super.copyFrom(base);
            instance.result = base.getResult();
            return (B)this;
        }


        public B withResult(ICommandResult result)
        {
            instance.result = result;
            return (B)this;
        }
    }
}
