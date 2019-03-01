/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.sensorhub.api.datastore.ObsKey;
import org.vast.util.Asserts;


class MVObsKey extends ObsKey
{
    long internalSourceID; // this internal ID identifies a combination of procedure+foi


    /*
     * this class can only be instantiated using builder
     */
    private MVObsKey()
    {
    }
    
    
    public long getInternalSourceID()
    {
        return internalSourceID;
    }
    
    
    static class Builder extends BaseBuilder<Builder, MVObsKey>
    {
        public Builder()
        {
            super(new MVObsKey());
        }


        public Builder withObsKey(ObsKey key)
        {
            instance.procedureID = key.getProcedureID();
            instance.foiID = key.getFoiID();
            instance.phenomenonTime = key.getPhenomenonTime();
            instance.resultTime = key.getResultTime();
            return this;
        }


        public Builder withInternalSourceID(long sourceID)
        {
            instance.internalSourceID = sourceID;
            return this;
        }
        
        
        @Override
        public MVObsKey build()
        {
            super.build();
            Asserts.checkNotNull(instance.internalSourceID, "internalSourceID");
            return instance;
        }
    }

}
