/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.tasking;

import org.sensorhub.api.procedure.ProcedureId;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
import org.vast.util.TimeExtent;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Immutable object containing information about a command stream/interface.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public class CommandStreamInfo implements ICommandStreamInfo
{
    protected ProcedureId procedureID;
    protected DataComponent recordStruct;
    protected DataEncoding recordEncoding;
    protected TimeExtent validTime;


    @Override
    public long getInternalID()
    {
        // should not be used until datastore assigns it
        throw new IllegalStateException("ID not assigned yet");
    }
    
    
    @Override
    public ProcedureId getProcedureID()
    {
        return procedureID;
    }


    @Override
    public String getCommandName()
    {
        return recordStruct.getName();
    }


    @Override
    public String getName()
    {
        return recordStruct.getLabel() != null ?
            recordStruct.getLabel() : getCommandName();
    }
    
    
    @Override
    public String getDescription()
    {
        return recordStruct.getDescription();
    }


    @Override
    public DataComponent getRecordStructure()
    {
        return recordStruct;
    }


    @Override
    public DataEncoding getRecordEncoding()
    {
        return recordEncoding;
    }


    @Override
    public TimeExtent getValidTime()
    {
        return validTime;
    }


    /*
     * Builder
     */
    public static class Builder extends CommandStreamInfoBuilder<Builder, CommandStreamInfo>
    {
        public Builder()
        {
            this.instance = new CommandStreamInfo();
        }

        public static Builder from(ICommandStreamInfo base)
        {
            return new Builder().copyFrom(base);
        }
    }


    @SuppressWarnings("unchecked")
    public static abstract class CommandStreamInfoBuilder<B extends CommandStreamInfoBuilder<B, T>, T extends CommandStreamInfo>
        extends BaseBuilder<T>
    {
        protected CommandStreamInfoBuilder()
        {
        }


        protected B copyFrom(ICommandStreamInfo base)
        {
            instance.procedureID = base.getProcedureID();
            instance.recordStruct = base.getRecordStructure();
            instance.recordEncoding = base.getRecordEncoding();
            instance.validTime = base.getValidTime();
            return (B)this;
        }


        public B withProcedure(ProcedureId procID)
        {
            instance.procedureID = procID;
            return (B)this;
        }


        public B withRecordDescription(DataComponent recordStruct)
        {
            instance.recordStruct = recordStruct;
            return (B)this;
        }


        public B withRecordEncoding(DataEncoding recordEncoding)
        {
            instance.recordEncoding = recordEncoding;
            return (B)this;
        }


        public B withValidTime(TimeExtent validTime)
        {
            instance.validTime = validTime;
            return (B)this;
        }


        @Override
        public T build()
        {
            Asserts.checkNotNull(instance.procedureID, "procedureID");
            Asserts.checkArgument(instance.procedureID.getInternalID() > 0, "procedure internalID must be > 0");
            Asserts.checkNotNull(instance.recordStruct, "recordStruct");
            Asserts.checkNotNull(instance.getCommandName(), "commandName");
            Asserts.checkNotNull(instance.recordEncoding, "recordEncoding");
            return super.build();
        }
    }
}
