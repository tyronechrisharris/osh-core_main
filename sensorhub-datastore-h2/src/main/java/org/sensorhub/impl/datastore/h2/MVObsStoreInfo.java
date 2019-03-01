/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.Duration;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


public class MVObsStoreInfo extends MVDataStoreInfo
{
    DataComponent recordStruct;
    DataEncoding recordEncoding;
    Duration samplingPeriod = Duration.ZERO;
    Duration resultPeriod = Duration.ZERO;
    
    
    private MVObsStoreInfo()
    {        
    }
    
    
    public DataComponent getRecordStruct()
    {
        return recordStruct;
    }


    public DataEncoding getRecordEncoding()
    {
        return recordEncoding;
    }


    public Duration getSamplingPeriod()
    {
        return samplingPeriod;
    }


    public Duration getResultPeriod()
    {
        return resultPeriod;
    }


    public static class Builder extends MVDataStoreInfo.BaseBuilder<Builder, MVObsStoreInfo>
    {
        
        public Builder()
        {
            super(new MVObsStoreInfo());
        }


        public Builder withRecordStructure(DataComponent recordStruct)
        {
            instance.recordStruct = recordStruct;
            return this;
        }


        public Builder withRecordEncoding(DataEncoding recordEncoding)
        {
            instance.recordEncoding = recordEncoding;
            return this;
        }


        public Builder withSamplingPeriod(Duration samplingPeriod)
        {
            instance.samplingPeriod = samplingPeriod;
            return this;
        }


        public Builder withResultPeriod(Duration resultPeriod)
        {
            instance.resultPeriod = resultPeriod;
            return this;
        }
    }
}
