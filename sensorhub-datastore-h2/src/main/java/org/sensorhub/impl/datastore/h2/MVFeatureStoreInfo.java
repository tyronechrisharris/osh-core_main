/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;


public class MVFeatureStoreInfo extends MVDataStoreInfo
{
    String featureUriPrefix;
    
    
    private MVFeatureStoreInfo()
    {        
    }
    

    public String getFeatureUriPrefix()
    {
        return featureUriPrefix;
    }
    
    
    public static class Builder extends MVDataStoreInfo.BaseBuilder<Builder, MVFeatureStoreInfo>
    {
        
        public Builder()
        {
            super(new MVFeatureStoreInfo());
        }


        public Builder withFeatureUriPrefix(String prefix)
        {
            instance.featureUriPrefix = prefix;
            return this;
        }
    }
}
