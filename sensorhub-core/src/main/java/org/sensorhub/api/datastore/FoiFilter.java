/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.

 Contributor(s): 
    Alexandre Robin "alex.robin@sensiasoft.com"
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;


/**
 * <p>
 * Immutable filter object for features of interest associated to observations.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2018
 */
public class FoiFilter extends FeatureFilter
{
    private FeatureFilter sampledFeatures;
    private ObsFilter observations;
    
    
    /*
     * this class can only be instantiated using builder
     */
    private FoiFilter() {}


    public FeatureFilter getSampledFeatures()
    {
        return sampledFeatures;
    }

    
    
    public ObsFilter getObservations()
    {
        return observations;
    }
    
    
    public static class Builder extends FeatureFilter.BaseBuilder<Builder, FoiFilter>
    {        
        public Builder()
        {
            super(new FoiFilter());
        }


        public Builder withSampledFeatures(FeatureFilter sampledFeatures)
        {
            instance.sampledFeatures = sampledFeatures;
            return this;
        }


        public Builder withObservations(ObsFilter observations)
        {
            instance.observations = observations;
            return this;
        }
    }
}
