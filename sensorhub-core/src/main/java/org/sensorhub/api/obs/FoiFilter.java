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

import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.sensorhub.api.feature.FeatureFilter;
import org.sensorhub.api.feature.FeatureFilterBase;
import org.sensorhub.api.resource.ResourceFilter;
import org.vast.ogc.gml.IGeoFeature;


/**
 * <p>
 * Immutable filter object for features of interest associated to observations.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2018
 */
public class FoiFilter extends FeatureFilterBase<IGeoFeature>
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
    
    
    /**
     * Computes the intersection (logical AND) between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    @Override
    public FoiFilter intersect(ResourceFilter<IGeoFeature> filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        
        return and((FoiFilter)filter, new Builder()).build();
    }
    
    
    protected <B extends FoiFilterBuilder<B, FoiFilter>> B and(FoiFilter otherFilter, B builder) throws EmptyFilterIntersection
    {
        super.intersect(otherFilter, builder);
        
        var sfFilter = this.sampledFeatures != null ? this.sampledFeatures.intersect(otherFilter.sampledFeatures) : otherFilter.sampledFeatures;
        if (sfFilter != null)
            builder.withSampledFeatures(sfFilter);
        
        var obsFilter = this.obsFilter != null ? this.obsFilter.intersect(otherFilter.obsFilter) : otherFilter.obsFilter;
        if (obsFilter != null)
            builder.withObservations(obsFilter);
        
        return builder;
    }
    
    
    /**
     * Deep clone this filter
     */
    public FoiFilter clone()
    {
        return Builder.from(this).build();
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
        
        
        /**
         * Builds a new filter using the provided filter as a base
         * @param base Filter used as base
         * @return The new builder
         */
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
        extends FeatureFilterBuilder<B, IGeoFeature, FoiFilter>
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
         * Select only FOIs that are sampling the features matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @param sampledFeatures Sampled features filter
         * @return The {@link FeatureFilter} builder for chaining
         */
        public FeatureFilter.NestedBuilder<B> withSampledFeatures()
        {
            return new FeatureFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    FoiFilterBuilder.this.withSampledFeatures(build());
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

        
        /**
         * Select only FOIs with observations matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link ObsFilter} builder for chaining
         */
        public ObsFilter.NestedBuilder<B> withObservations()
        {
            return new ObsFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    FoiFilterBuilder.this.withObservations(build());
                    return (B)FoiFilterBuilder.this;
                }                
            };
        }
    }
}
