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

import org.sensorhub.api.feature.FeatureFilter;

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
    protected FeatureFilter sampledFeatures;
    protected ObsFilter obsFilter;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected FoiFilter() {}


    public FeatureFilter getSampledFeatures()
    {
        return sampledFeatures;
    }
    
    
    public ObsFilter getObservationFilter()
    {
        return obsFilter;
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends FoiFilterBuilder<Builder, FoiFilter>
    {
        public Builder()
        {
            super(new FoiFilter());
        }
        
        public static Builder from(FoiFilter base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    /*
     * Nested builder for use within another builder
     */
    public static abstract class NestedBuilder<B> extends FoiFilterBuilder<NestedBuilder<B>, FoiFilter>
    {
        B parent;
        
        public NestedBuilder(B parent)
        {
            super(new FoiFilter());
            this.parent = parent;
        }
                
        public abstract B done();
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class FoiFilterBuilder<
            B extends FoiFilterBuilder<B, F>,
            F extends FoiFilter>
        extends FeatureFilterBuilder<B, FoiFilter>
    {    
        
        protected FoiFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        protected B copyFrom(FoiFilter base)
        {
            super.copyFrom(base);
            instance.sampledFeatures = base.sampledFeatures;
            instance.obsFilter = base.obsFilter;
            return (B)this;
        }
        

        /**
         * Select only FOIs that are sampling the features matching the filter
         * @param sampledFeatures Sampled features filter
         * @return This builder for chaining
         */
        public B withSampledFeatures(FeatureFilter sampledFeatures)
        {
            instance.sampledFeatures = sampledFeatures;
            return (B)this;
        }
        

        /**
         * Select only FOIs that are sampling the features matching the filter
         * @param sampledFeatures Sampled features filter
         * @return This builder for chaining
         */
        public B withSampledFeatures(FeatureFilter.Builder sampledFeatures)
        {
            return withSampledFeatures(sampledFeatures.build());
        }

        
        /**
         * Select only FOIs with observations matching the filter
         * @return The {@link ObsFilter} builder for chaining
         */
        public ObsFilter.NestedBuilder<B> withObservations()
        {
            return new ObsFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    FoiFilterBuilder.this.instance.obsFilter = build();
                    return (B)FoiFilterBuilder.this;
                }                
            };
        }
        

        /**
         * Select only FOIs with observations matching the filter
         * @param filter Observation filter
         * @return This builder for chaining
         */
        public B withObservations(ObsFilter filter)
        {
            instance.obsFilter = filter;
            return (B)this;
        }
    }
}
