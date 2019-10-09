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
    FeatureId procedureID;
    int recordVersion;
    DataComponent recordStruct;
    DataEncoding recordEncoding;
    
    
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
    
    
    public static Builder builder()
    {
        return new Builder();
    }

    
    public static class Builder extends BaseBuilder<DataStreamInfo>
    {
        protected Builder()
        {
            super(new DataStreamInfo());
        }
        
        
        public Builder withProcedure(FeatureId procID)
        {
            instance.procedureID = procID;
            return this;
        }
        
        
        public Builder withProcedure(long internalID)
        {
            instance.procedureID = new FeatureId(internalID);
            return this;
        }
        
        
        public Builder withProcedure(String uid)
        {
            instance.procedureID = new FeatureId(0, uid);
            return this;
        }
        
        
        public Builder withRecordVersion(int version)
        {
            instance.recordVersion = version;
            return this;
        }


        public Builder withRecordDescription(DataComponent recordStruct)
        {
            instance.recordStruct = recordStruct;
            return this;
        }


        public Builder withRecordEncoding(DataEncoding recordEncoding)
        {
            instance.recordEncoding = recordEncoding;
            return this;
        }
        
        
        public DataStreamInfo build()
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
