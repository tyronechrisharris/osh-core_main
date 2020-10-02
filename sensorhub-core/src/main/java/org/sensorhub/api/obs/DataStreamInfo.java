/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.obs;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.sensorhub.api.procedure.ProcedureId;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
import org.vast.util.TimeExtent;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Immutable object containing information about a data stream of observations.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 19, 2019
 */
public class DataStreamInfo implements IDataStreamInfo
{
    protected ProcedureId procedureID;
    protected int recordVersion;
    protected DataComponent recordStruct;
    protected DataEncoding recordEncoding;


    @Override
    public ProcedureId getProcedureID()
    {
        return procedureID;
    }


    @Override
    public String getOutputName()
    {
        return recordStruct.getName();
    }


    @Override
    public int getRecordVersion()
    {
        return recordVersion;
    }


    @Override
    public DataComponent getRecordDescription()
    {
        return recordStruct;
    }


    @Override
    public DataEncoding getRecordEncoding()
    {
        return recordEncoding;
    }


    /*
     * Builder
     */
    public static class Builder extends DataStreamInfoBuilder<Builder, DataStreamInfo>
    {
        public Builder()
        {
            this.instance = new DataStreamInfo();
        }

        public static Builder from(IDataStreamInfo base)
        {
            return new Builder().copyFrom(base);
        }
    }


    @SuppressWarnings("unchecked")
    public static abstract class DataStreamInfoBuilder<B extends DataStreamInfoBuilder<B, T>, T extends DataStreamInfo>
        extends BaseBuilder<T>
    {
        protected DataStreamInfoBuilder()
        {
        }


        protected B copyFrom(IDataStreamInfo base)
        {
            instance.procedureID = base.getProcedureID();
            instance.recordStruct = base.getRecordDescription();
            instance.recordEncoding = base.getRecordEncoding();
            instance.recordVersion = base.getRecordVersion();
            return (B)this;
        }


        public B withProcedure(ProcedureId procID)
        {
            instance.procedureID = procID;
            return (B)this;
        }


        public B withRecordVersion(int version)
        {
            instance.recordVersion = version;
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


        @Override
        public T build()
        {
            Asserts.checkNotNull(instance.procedureID, "procedureID");
            Asserts.checkArgument(instance.procedureID.getInternalID() > 0, "procedure internalID must be > 0");
            //Asserts.checkArgument(!Strings.isNullOrEmpty(instance.procedureID.uniqueID), "procedure UID must be set");
            Asserts.checkNotNull(instance.recordStruct, "recordStruct");
            Asserts.checkNotNull(instance.getOutputName(), "outputName/recordName");
            Asserts.checkNotNull(instance.recordEncoding, "recordEncoding");
            return super.build();
        }
    }


    @Override
    public TimeExtent getPhenomenonTimeRange()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public TimeExtent getResultTimeRange()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean hasDiscreteResultTimes()
    {
        return false;
    }


    @Override
    public Map<Instant, TimeExtent> getDiscreteResultTimes()
    {
        return Collections.emptyMap();
    }
}
