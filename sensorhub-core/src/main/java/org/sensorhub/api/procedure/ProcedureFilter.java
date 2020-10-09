/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.sensorhub.api.feature.FeatureFilterBase;
import org.sensorhub.api.obs.DataStreamFilter;
import org.sensorhub.api.obs.FoiFilter;
import org.sensorhub.api.resource.ResourceFilter;
import org.vast.ogc.om.IProcedure;


/**
 * <p>
 * Immutable filter object for procedures (e.g. sensors, actuators, procedure groups etc.).<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 2, 2018
 */
public class ProcedureFilter extends FeatureFilterBase<IProcedure>
{
    protected ProcedureFilter parentFilter;
    protected ProcedureFilter memberFilter;
    protected DataStreamFilter dataStreamFilter;
    
    // Note that this FoiFilter is different from DataStreamFilter/ObsFilter/FoiFilter
    // we also need a foi filter here because fois can be associated to a procedure
    // before there are any observations about them
    protected FoiFilter foiFilter;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected ProcedureFilter() {}
    
    
    public ProcedureFilter getParentFilter()
    {
        return parentFilter;
    }
    
    
    public ProcedureFilter getMemberFilter()
    {
        return memberFilter;
    }
    
    
    public DataStreamFilter getDataStreamFilter()
    {
        return dataStreamFilter;
    }


    public FoiFilter getFoiFilter()
    {
        return foiFilter;
    }
    
    
    /**
     * Computes the intersection (logical AND) between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    @Override
    public ProcedureFilter intersect(ResourceFilter<IProcedure> filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        
        return intersect((ProcedureFilter)filter, new Builder()).build();
    }
    
    
    protected <B extends ProcedureFilterBuilder<B, ProcedureFilter>> B intersect(ProcedureFilter otherFilter, B builder) throws EmptyFilterIntersection
    {
        super.intersect(otherFilter, builder);
        
        var parentFilter = this.parentFilter != null ? this.parentFilter.intersect(otherFilter.parentFilter) : otherFilter.parentFilter;
        if (parentFilter != null)
            builder.withParents(parentFilter);
        
        var memberFilter = this.memberFilter != null ? this.memberFilter.intersect(otherFilter.memberFilter) : otherFilter.memberFilter;
        if (parentFilter != null)
            builder.withMembers(memberFilter);
        
        var dataStreamFilter = this.dataStreamFilter != null ? this.dataStreamFilter.intersect(otherFilter.dataStreamFilter) : otherFilter.dataStreamFilter;
        if (dataStreamFilter != null)
            builder.withDataStreams(dataStreamFilter);
        
        var foiFilter = this.foiFilter != null ? this.foiFilter.intersect(otherFilter.foiFilter) : otherFilter.foiFilter;
        if (foiFilter != null)
            builder.withFois(foiFilter);
        
        return builder;
    }
    
    
    /**
     * Deep clone this filter
     */
    public ProcedureFilter clone()
    {
        return Builder.from(this).build();
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends ProcedureFilterBuilder<Builder, ProcedureFilter>
    {
        public Builder()
        {
            super(new ProcedureFilter());
        }
        
        /**
         * Builds a new filter using the provided filter as a base
         * @param base Filter used as base
         * @return The new builder
         */
        public static Builder from(ProcedureFilter base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    /*
     * Nested builder for use within another builder
     */
    public static abstract class NestedBuilder<B> extends ProcedureFilterBuilder<NestedBuilder<B>, ProcedureFilter>
    {
        B parent;
        
        public NestedBuilder(B parent)
        {
            super(new ProcedureFilter());
            this.parent = parent;
        }
                
        public abstract B done();
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class ProcedureFilterBuilder<
            B extends ProcedureFilterBuilder<B, F>,
            F extends ProcedureFilter>
        extends FeatureFilterBaseBuilder<B, IProcedure, F>
    {        
        
        protected ProcedureFilterBuilder(F instance)
        {
            super(instance);
        }
                
        
        protected B copyFrom(F base)
        {
            super.copyFrom(base);
            instance.parentFilter = base.parentFilter;
            instance.memberFilter = base.memberFilter;
            instance.dataStreamFilter = base.dataStreamFilter;
            instance.foiFilter = base.foiFilter;
            return (B)this;
        }
        
        
        /**
         * Select only procedures belonging to the matching groups
         * @param filter Parent procedure filter
         * @return This builder for chaining
         */
        public B withParents(ProcedureFilter filter)
        {
            instance.parentFilter = filter;
            return (B)this;
        }

        
        /**
         * Keep only procedures belonging to the matching groups.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link ProcedureFilter} builder for chaining
         */
        public ProcedureFilter.NestedBuilder<B> withParents()
        {
            return new ProcedureFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ProcedureFilterBuilder.this.withParents(build());
                    return (B)ProcedureFilterBuilder.this;
                }                
            };
        }
        
        
        /**
         * Select only procedure groups with the matching members
         * @param filter Member procedure filter
         * @return This builder for chaining
         */
        public B withMembers(ProcedureFilter filter)
        {
            instance.memberFilter = filter;
            return (B)this;
        }

        
        /**
         * Keep only procedure groups with the matching members.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link ProcedureFilter} builder for chaining
         */
        public ProcedureFilter.NestedBuilder<B> withMembers()
        {
            return new ProcedureFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ProcedureFilterBuilder.this.withMembers(build());
                    return (B)ProcedureFilterBuilder.this;
                }                
            };
        }
        
        
        /**
         * Select only procedures with data streams matching the filter
         * @param filter Data stream filter
         * @return This builder for chaining
         */
        public B withDataStreams(DataStreamFilter filter)
        {
            instance.dataStreamFilter = filter;
            return (B)this;
        }

        
        /**
         * Keep only procedures from data streams matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link DataStreamFilter} builder for chaining
         */
        public DataStreamFilter.NestedBuilder<B> withDataStreams()
        {
            return new DataStreamFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ProcedureFilterBuilder.this.withDataStreams(build());
                    return (B)ProcedureFilterBuilder.this;
                }                
            };
        }
        

        /**
         * Select only procedures with features of interest matching the filter
         * @param filter Features of interest filter
         * @return This builder for chaining
         */
        public B withFois(FoiFilter filter)
        {
            instance.foiFilter = filter;
            return (B)this;
        }

        
        /**
         * Select only procedures with features of interest matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link FoiFilter} builder for chaining
         */
        public FoiFilter.NestedBuilder<B> withFois()
        {
            return new FoiFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ProcedureFilterBuilder.this.withFois(build());
                    return (B)ProcedureFilterBuilder.this;
                }                
            };
        }
    }
}
