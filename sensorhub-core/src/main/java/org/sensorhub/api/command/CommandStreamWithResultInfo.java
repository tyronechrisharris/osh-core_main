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

import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Immutable object containing information about a command stream/interface
 * with support for result data.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public class CommandStreamWithResultInfo extends CommandStreamInfo
{
    protected DataComponent resultStruct;
    protected DataEncoding resultEncoding;


    @Override
    public DataComponent getResultStructure()
    {
        return resultStruct;
    }


    @Override
    public DataEncoding getResultEncoding()
    {
        return resultEncoding;
    }


    @Override
    public boolean hasResult()
    {
        return true;
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends CommandStreamWithResultInfoBuilder<Builder, CommandStreamWithResultInfo>
    {
        public Builder()
        {
            this.instance = new CommandStreamWithResultInfo();
        }

        public static Builder from(ICommandStreamInfo base)
        {
            return new Builder().copyFrom(base);
        }
    }


    @SuppressWarnings("unchecked")
    public abstract static class CommandStreamWithResultInfoBuilder<B extends CommandStreamWithResultInfoBuilder<B, T>, T extends CommandStreamWithResultInfo>
        extends CommandStreamInfoBuilder<B, T>
    {
        protected CommandStreamWithResultInfoBuilder()
        {
        }


        @Override
        protected B copyFrom(ICommandStreamInfo base)
        {
            super.copyFrom(base);
            instance.resultStruct = base.getResultStructure();
            instance.resultEncoding = base.getResultEncoding();
            return (B)this;
        }


        public B withResultDescription(DataComponent resultStruct)
        {
            instance.resultStruct = resultStruct;
            return (B)this;
        }


        public B withResultEncoding(DataEncoding resultEncoding)
        {
            instance.resultEncoding = resultEncoding;
            return (B)this;
        }
    }
}
