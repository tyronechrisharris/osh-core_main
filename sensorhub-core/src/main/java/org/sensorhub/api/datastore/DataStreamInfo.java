/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
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
public class DataStreamInfo
{
    protected FeatureId procedureID;
    protected int recordVersion;
    protected DataComponent recordStruct;
    protected DataEncoding recordEncoding;
    
    
    /**
     * @return The identifier of the procedure that generated this data stream
     */
    public FeatureId getProcedure()
    {
        return procedureID;
    }


    /**
     * @return The name of the procedure output that is/was the source of
     * this data stream
     */
    public String getOutputName()
    {
        return recordStruct.getName();
    }


    /**
     * @return The version of the output record schema used in this data stream
     */
    public int getRecordVersion()
    {
        return recordVersion;
    }


    /**
     * @return The data stream record structure
     */
    public DataComponent getRecordDescription()
    {
        return recordStruct;
    }


    /**
     * @return The recommended encoding for the data stream
     */
    public DataEncoding getRecommendedEncoding()
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
        
        public static Builder from(DataStreamInfo base)
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
        
        
        protected B copyFrom(DataStreamInfo base)
        {
            instance.procedureID = base.procedureID;
            instance.recordStruct = base.recordStruct;
            instance.recordEncoding = base.recordEncoding;
            instance.recordVersion = base.recordVersion;
            return (B)this;
        }
        
        
        public B withProcedure(FeatureId procID)
        {
            instance.procedureID = procID;
            return (B)this;
        }
        
        
        public B withProcedure(long internalID)
        {
            instance.procedureID = new FeatureId(internalID);
            return (B)this;
        }
        
        
        public B withProcedure(String uid)
        {
            instance.procedureID = new FeatureId(0, uid);
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
        
        
        public T build()
        {
            Asserts.checkNotNull(instance.procedureID, "procedureID");
            Asserts.checkArgument(instance.procedureID.internalID > 0, "procedure internalID must be > 0");
            //Asserts.checkArgument(!Strings.isNullOrEmpty(instance.procedureID.uniqueID), "procedure UID must be set");
            Asserts.checkNotNull(instance.recordStruct, "recordStruct");
            Asserts.checkNotNull(instance.getOutputName(), "outputName/recordName");
            Asserts.checkNotNull(instance.recordEncoding, "recordEncoding");
            return super.build();
        }
    }
}
