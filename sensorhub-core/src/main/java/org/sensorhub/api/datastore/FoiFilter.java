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

    
    
    public ObsFilter getObservationsFilter()
    {
        return observations;
    }
    
    
    public static Builder builder()
    {
        return new Builder();
    }
    
    
    public static class Builder extends FeatureFilter.Builder<Builder, FoiFilter>
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
