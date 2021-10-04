/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore.obs;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.function.Predicate;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.utils.FilterUtils;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.BaseBuilder;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Longs;


/**
 * <p>
 * Immutable filter object for observations.<br/>
 * There is an implicit AND between all filter parameters.
 * </p>
 *
 * @author Alex Robin
 * @date Jan 16, 2018
 */
public class ObsFilter implements IQueryFilter, Predicate<IObsData>
{
    protected SortedSet<BigInteger> internalIDs;
    protected TemporalFilter phenomenonTime;
    protected TemporalFilter resultTime;
    protected SpatialFilter phenomenonLocation;
    protected DataStreamFilter dataStreamFilter;
    protected FoiFilter foiFilter;
    protected Predicate<IObsData> valuePredicate;
    protected long limit = Long.MAX_VALUE;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected ObsFilter() {}


    public SortedSet<BigInteger> getInternalIDs()
    {
        return internalIDs;
    }
    
    
    public TemporalFilter getPhenomenonTime()
    {
        return phenomenonTime;
    }


    public TemporalFilter getResultTime()
    {
        return resultTime;
    }


    public SpatialFilter getPhenomenonLocation()
    {
        return phenomenonLocation;
    }


    public DataStreamFilter getDataStreamFilter()
    {
        return dataStreamFilter;
    }


    public FoiFilter getFoiFilter()
    {
        return foiFilter;
    }


    public Predicate<IObsData> getValuePredicate()
    {
        return valuePredicate;
    }


    @Override
    public long getLimit()
    {
        return limit;
    }


    @Override
    public boolean test(IObsData obs)
    {
        return (testPhenomenonTime(obs) &&
                testResultTime(obs) &&
                testPhenomenonLocation(obs) &&
                testValuePredicate(obs));
    }
    
    
    public boolean testPhenomenonTime(IObsData v)
    {
        return (phenomenonTime == null ||
                phenomenonTime.test(v.getPhenomenonTime()));
    }
    
    
    public boolean testResultTime(IObsData v)
    {
        return (resultTime == null ||
                resultTime.test(v.getResultTime()));
    }
    
    
    public boolean testPhenomenonLocation(IObsData v)
    {
        return (phenomenonLocation == null || 
                (v.getPhenomenonLocation() != null &&
                phenomenonLocation.test(v.getPhenomenonLocation())));            
    }
    
    
    public boolean testValuePredicate(IObsData v)
    {
        return (valuePredicate == null ||
                valuePredicate.test(v));
    }
    
    
    /**
     * Computes the intersection (logical AND) between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    public ObsFilter intersect(ObsFilter filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        
        var builder = new Builder();
        
        var internalIDs = FilterUtils.intersect(this.internalIDs, filter.internalIDs);
        if (internalIDs != null)
            builder.withInternalIDs(internalIDs);
        
        var phenomenonTime = this.phenomenonTime != null ? this.phenomenonTime.intersect(filter.phenomenonTime) : filter.phenomenonTime;
        if (phenomenonTime != null)
            builder.withPhenomenonTime(phenomenonTime);
        
        var resultTime = this.resultTime != null ? this.resultTime.intersect(filter.resultTime) : filter.resultTime;
        if (resultTime != null)
            builder.withResultTime(resultTime);
        
        var phenomenonLocation = this.phenomenonLocation != null ? this.phenomenonLocation.intersect(filter.phenomenonLocation) : filter.phenomenonLocation;
        if (phenomenonLocation != null)
            builder.withPhenomenonLocation(phenomenonLocation);
        
        var dataStreamFilter = this.dataStreamFilter != null ? this.dataStreamFilter.intersect(filter.dataStreamFilter) : filter.dataStreamFilter;
        if (dataStreamFilter != null)
            builder.withDataStreams(dataStreamFilter);
        
        var foiFilter = this.foiFilter != null ? this.foiFilter.intersect(filter.foiFilter) : filter.foiFilter;
        if (foiFilter != null)
            builder.withFois(foiFilter);
        
        var valuePredicate = this.valuePredicate != null ? this.valuePredicate.and(filter.valuePredicate) : filter.valuePredicate;
        if (valuePredicate != null)
            builder.withValuePredicate(valuePredicate);
        
        var limit = Math.min(this.limit, filter.limit);
        builder.withLimit(limit);
        
        return builder.build();
    }
    
    
    /**
     * Deep clone this filter
     */
    public ObsFilter clone()
    {
        return Builder.from(this).build();
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true, true);
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends ObsFilterBuilder<Builder, ObsFilter>
    {
        public Builder()
        {
            this.instance = new ObsFilter();
        }
        
        /**
         * Builds a new filter using the provided filter as a base
         * @param base Filter used as base
         * @return The new builder
         */
        public static Builder from(ObsFilter base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    /*
     * Nested builder for use within another builder
     */
    public static abstract class NestedBuilder<B> extends ObsFilterBuilder<NestedBuilder<B>, ObsFilter>
    {
        B parent;
        
        public NestedBuilder(B parent)
        {
            this.parent = parent;
            this.instance = new ObsFilter();
        }
                
        public abstract B done();
    }


    @SuppressWarnings("unchecked")
    public static abstract class ObsFilterBuilder<
            B extends ObsFilterBuilder<B, T>,
            T extends ObsFilter>
        extends BaseBuilder<T>
    {
        
        protected ObsFilterBuilder()
        {
        }
        
        
        /**
         * Init this builder with settings from the provided filter
         * @param base
         * @return This builder for chaining
         */
        public B copyFrom(ObsFilter base)
        {
            instance.internalIDs = base.internalIDs;
            instance.phenomenonTime = base.phenomenonTime;
            instance.resultTime = base.resultTime;
            instance.phenomenonLocation = base.phenomenonLocation;
            instance.dataStreamFilter = base.dataStreamFilter;
            instance.foiFilter = base.foiFilter;
            instance.valuePredicate = base.valuePredicate;
            instance.limit = base.limit;
            return (B)this;
        }
        
        
        /**
         * Keep only observations with specific internal IDs.
         * @param ids One or more internal IDs to select
         * @return This builder for chaining
         */
        public B withInternalIDs(BigInteger... ids)
        {
            return withInternalIDs(Arrays.asList(ids));
        }
        
        
        /**
         * Keep only observations with specific internal IDs.
         * @param ids Collection of internal IDs
         * @return This builder for chaining
         */
        public B withInternalIDs(Collection<BigInteger> ids)
        {
            instance.internalIDs = ImmutableSortedSet.copyOf(ids);
            return (B)this;
        }


        /**
         * Keep only observations whose phenomenon time matches the temporal filter.
         * @param filter Temporal filtering options
         * @return This builder for chaining
         */
        public B withPhenomenonTime(TemporalFilter filter)
        {
            instance.phenomenonTime = filter;
            return (B)this;
        }

        
        /**
         * Keep only observations whose phenomenon time matches the temporal filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link TemporalFilter} builder for chaining
         */
        public TemporalFilter.NestedBuilder<B> withPhenomenonTime()
        {
            return new TemporalFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ObsFilterBuilder.this.withPhenomenonTime(build());
                    return (B)ObsFilterBuilder.this;
                }                
            };
        }


        /**
         * Keep only observations whose phenomenon time is within the given period.<br/>
         * The phenomenon time is the time when the phenomenon being measured happened.
         * @param begin Beginning of desired period
         * @param end End of desired period
         * @return This builder for chaining
         */
        public B withPhenomenonTimeDuring(Instant begin, Instant end)
        {
            return withPhenomenonTime(new TemporalFilter.Builder()
                .withRange(begin, end)
                .build());
        }


        /**
         * Keep only observations whose result time matches the temporal filter.
         * @param filter Temporal filtering options
         * @return This builder for chaining
         */
        public B withResultTime(TemporalFilter filter)
        {
            instance.resultTime = filter;
            return (B)this;
        }


        /**
         * Keep only observations whose result time is within the given period.<br/>
         * The result time is the time when the data resulting from the observation
         * was generated.
         * @param begin Beginning of desired period
         * @param end End of desired period
         * @return This builder for chaining
         */
        public B withResultTimeDuring(Instant begin, Instant end)
        {
            return withResultTime(new TemporalFilter.Builder()
                    .withRange(begin, end)
                    .build());
        }
        
        
        /**
         * Keep only observation(s) with the latest result time.<br/>
         * For many sensors, this means only the last observation collected.<br/>
         * For models, this means observations generated during the latest run.
         * @return This builder for chaining
         */
        public B withLatestResult()
        {
            return withResultTime(new TemporalFilter.Builder()
                .withLatestTime().build());
        }


        /**
         * Keep only observations whose phenomenon location, if any, matches the spatial filter.
         * @param filter Spatial filtering options
         * @return This builder for chaining
         */
        public B withPhenomenonLocation(SpatialFilter filter)
        {
            instance.phenomenonLocation = filter;
            return (B)this;
        }

        
        /**
         * Keep only observations whose phenomenon location, if any, matches the spatial filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link SpatialFilter} builder for chaining
         */
        public SpatialFilter.NestedBuilder<B> withPhenomenonLocation()
        {
            return new SpatialFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ObsFilterBuilder.this.withPhenomenonLocation(build());
                    return (B)ObsFilterBuilder.this;
                }                
            };
        }


        /**
         * Keep only observations from data streams matching the filter.
         * @param filter Filter to select desired data streams
         * @return This builder for chaining
         */
        public B withDataStreams(DataStreamFilter filter)
        {
            instance.dataStreamFilter = filter;
            return (B)this;
        }
        
        
        /**
         * Keep only observations from data streams matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link DataStreamFilter} builder for chaining
         */
        public DataStreamFilter.NestedBuilder<B> withDataStreams()
        {
            return new DataStreamFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ObsFilterBuilder.this.withDataStreams(build());
                    return (B)ObsFilterBuilder.this;
                }                
            };
        }


        /**
         * Keep only observations from specific data streams.
         * @param ids Internal IDs of one or more data streams
         * @return This builder for chaining
         */
        public B withDataStreams(long... ids)
        {
            return withDataStreams(Longs.asList(ids));
        }


        /**
         * Keep only observations from specific data streams.
         * @param ids Collection of internal IDs of data streams
         * @return This builder for chaining
         */
        public B withDataStreams(Collection<Long> ids)
        {
            return withDataStreams(new DataStreamFilter.Builder()
                .withInternalIDs(ids)
                .build());
        }


        /**
         * Keep only observations from procedures matching the filter.
         * @param filter Filter to select desired procedures
         * @return This builder for chaining
         */
        public B withProcedures(ProcedureFilter filter)
        {
            return withDataStreams(new DataStreamFilter.Builder()
                .withProcedures(filter)
                .build());
        }
        
        
        /**
         * Keep only observations from procedures matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link ProcedureFilter} builder for chaining
         */
        public ProcedureFilter.NestedBuilder<B> withProcedures()
        {
            return new ProcedureFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ObsFilterBuilder.this.withProcedures(build());
                    return (B)ObsFilterBuilder.this;
                }                
            };
        }


        /**
         * Keep only observations from specific procedures (including all outputs).
         * @param procIDs Internal IDs of one or more procedures
         * @return This builder for chaining
         */
        public B withProcedures(long... procIDs)
        {
            return withProcedures(Longs.asList(procIDs));
        }


        /**
         * Keep only observations from specific procedures (including all outputs).
         * @param procIDs Collection of internal IDs of procedures
         * @return This builder for chaining
         */
        public B withProcedures(Collection<Long> procIDs)
        {
            return withDataStreams(new DataStreamFilter.Builder()
                .withProcedures(procIDs)
                .build());
        }


        /**
         * Keep only observations produced by certain outputs of a specific procedure
         * @param procID Internal ID of the procedure
         * @param outputNames Names of one or more outputs of interest
         * @return This builder for chaining
         */
        public B withProcedure(long procID, String... outputNames)
        {
            return withDataStreams(new DataStreamFilter.Builder()
                .withProcedures(procID)
                .withOutputNames(outputNames)
                .build());
        }


        /**
         * Keep only observations of features of interest matching the filter.
         * @param filter Filter to select features of interest
         * @return This builder for chaining
         */
        public B withFois(FoiFilter filter)
        {
            instance.foiFilter = filter;
            return (B)this;
        }

        
        /**
         * Keep only observations of features of interest matching the filter.<br/>
         * Call done() on the nested builder to go back to main builder.
         * @return The {@link FoiFilter} builder for chaining
         */
        public FoiFilter.NestedBuilder<B> withFois()
        {
            return new FoiFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    withFois(build());
                    return (B)ObsFilterBuilder.this;
                }                
            };
        }


        /**
         * Keep only observations of the specified features of interests
         * @param foiIDs Internal IDs of one or more fois
         * @return This builder for chaining
         */
        public B withFois(long... foiIDs)
        {
            return withFois(Longs.asList(foiIDs));
        }


        /**
         * Keep only observations of the specified features of interests
         * @param foiIDs Collection of FOI internal IDs
         * @return This builder for chaining
         */
        public B withFois(Collection<Long> foiIDs)
        {
            return withFois(new FoiFilter.Builder()
                .withInternalIDs(foiIDs)
                .build());
        }


        /**
         * Keep only the observations whose data matches the predicate
         * @param valuePredicate The predicate to test the observation data
         * @return This builder for chaining
         */
        public B withValuePredicate(Predicate<IObsData> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return (B)this;
        }


        /**
         * Sets the maximum number of observations to retrieve
         * @param limit Max observations count
         * @return This builder for chaining
         */
        public B withLimit(long limit)
        {
            instance.limit = limit;
            return (B)this;
        }
    }
}
