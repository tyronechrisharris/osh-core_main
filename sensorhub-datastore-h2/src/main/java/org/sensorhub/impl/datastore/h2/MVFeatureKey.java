/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.sensorhub.api.datastore.FeatureKey;
import org.vast.util.Asserts;


class MVFeatureKey extends FeatureKey
{
    long internalID;
    
    
    /*
     * this class can only be instantiated using builder
     */
    private MVFeatureKey()
    {
    }


    public long getInternalID()
    {
        return internalID;
    }
    
    
    static class Builder extends BaseBuilder<Builder, MVFeatureKey>
    {
        public Builder()
        {
            super(new MVFeatureKey());
        }


        public Builder withFeatureKey(FeatureKey key)
        {
            instance.uniqueID = key.getUniqueID();
            instance.validStartTime = key.getValidStartTime();
            return this;
        }


        public Builder withInternalID(long internalID)
        {
            instance.internalID = internalID;
            return this;
        }
        
        
        @Override
        public MVFeatureKey build()
        {
            Asserts.checkNotNull(instance.internalID, "internalID");
            Asserts.checkNotNull(instance.validStartTime, "validStartTime");
            return instance;
        }
    }

}
