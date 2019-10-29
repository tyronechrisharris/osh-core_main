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

import java.time.Instant;
import java.util.function.Predicate;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.BaseBuilder;


/**
 * <p>
 * Immutable filter object for observations.<br/>
 * There is an implicit AND between all filter parameters.
 * </p>
 *
 * @author Alex Robin
 * @date Jan 16, 2018
 */
public class ObsFilter implements IQueryFilter, Predicate<ObsData>
{
    protected TemporalFilter phenomenonTime;
    protected TemporalFilter resultTime;
    protected SpatialFilter phenomenonLocation;
    protected DataStreamFilter dataStreams;
    protected FoiFilter foiFilter;
    protected Predicate<ObsKey> keyPredicate;
    protected Predicate<ObsData> valuePredicate;
    protected long limit = Long.MAX_VALUE;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected ObsFilter() {}
    
    
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
        return dataStreams;
    }


    public FoiFilter getFoiFilter()
    {
        return foiFilter;
    }


    public Predicate<ObsKey> getKeyPredicate()
    {
        return keyPredicate;
    }


    public Predicate<ObsData> getValuePredicate()
    {
        return valuePredicate;
    }


    @Override
    public long getLimit()
    {
        return limit;
    }
    
    
    public boolean testPhenomenonTime(ObsKey k)
    {
        return (phenomenonTime == null ||
                phenomenonTime.test(k.getPhenomenonTime()));
    }
    
    
    public boolean testResultTime(ObsKey k)
    {
        return (resultTime == null ||
                resultTime.test(k.getResultTime()));
    }
    
    
    public boolean testPhenomenonLocation(ObsData v)
    {
        return (phenomenonLocation == null || 
                (v.getPhenomenonLocation() != null &&
                phenomenonLocation.test(v.getPhenomenonLocation())));            
    }
    
    
    public boolean testKeyPredicate(ObsKey k)
    {
        return (keyPredicate == null ||
                keyPredicate.test(k));
    }
    
    
    public boolean testValuePredicate(ObsData v)
    {
        return (valuePredicate == null ||
                valuePredicate.test(v));
    }


    @Override
    public boolean test(ObsData obs)
    {
        return (testPhenomenonLocation(obs) &&
                testValuePredicate(obs));
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
        
        public static Builder from(ObsFilter base)
        {
            return new Builder().copyFrom(base);
        }
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
        
        
        protected B copyFrom(ObsFilter base)
        {
            instance.phenomenonTime = base.phenomenonTime;
            instance.resultTime = base.resultTime;
            instance.phenomenonLocation = base.phenomenonLocation;
            instance.dataStreams = base.dataStreams;
            instance.foiFilter = base.foiFilter;
            instance.keyPredicate = base.keyPredicate;
            instance.valuePredicate = base.valuePredicate;
            instance.limit = base.limit;
            return (B)this;
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
            instance.phenomenonTime = new TemporalFilter.Builder()
                .withRange(begin, end)
                .build();
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
            instance.resultTime = new TemporalFilter.Builder()
                    .withRange(begin, end)
                    .build();
            return (B)this;
        }
        
        
        /**
         * Keep only observation(s) with the latest result time.<br/>
         * For many sensors, this means only the last observation collected.<br/>
         * For models, this means observations generated during the latest run.
         * @return This builder for chaining
         */
        public B withLatestResult()
        {
            instance.resultTime = new TemporalFilter.Builder()
                .withLatestTime().build();
            return (B)this;
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
         * Keep only observations whose phenomenon location, if any, matches the spatial filter.
         * @param filter Spatial filtering options
         * @return This builder for chaining
         */
        public B withPhenomenonLocation(SpatialFilter.Builder filter)
        {
            return withPhenomenonLocation(filter.build());
        }


        /**
         * Keep only observations from data streams matching the filter.
         * @param filter Filter to select desired data streams
         * @return This builder for chaining
         */
        public B withDataStreams(DataStreamFilter filter)
        {
            instance.dataStreams = filter;
            return (B)this;
        }


        /**
         * Keep only observations from data streams matching the filter.
         * @param filter Filter to select desired data streams
         * @return This builder for chaining
         */
        public B withDataStreams(DataStreamFilter.Builder filter)
        {
            return withDataStreams(filter.build());
        }


        /**
         * Keep only observations from specific data streams.
         * @param ids Internal IDs of one or more data streams
         * @return This builder for chaining
         */
        public B withDataStreams(Long... ids)
        {
            instance.dataStreams = new DataStreamFilter.Builder()
                .withInternalIDs(ids)
                .build();
            return (B)this;
        }


        /**
         * Keep only observations from specific procedures (including all outputs).
         * @param procIDs Internal IDs of one or more procedures
         * @return This builder for chaining
         */
        public B withProcedures(Long... procIDs)
        {
            instance.dataStreams = new DataStreamFilter.Builder()
                .withProcedures(procIDs)
                .build();
            return (B)this;
        }


        /**
         * Keep only observations produced by certain outputs of a specific procedure
         * @param procID Internal ID of the procedure
         * @param outputNames Names of one or more outputs of interest
         * @return This builder for chaining
         */
        public B withProcedure(long procID, String... outputNames)
        {
            instance.dataStreams = new DataStreamFilter.Builder()
                .withProcedures(procID)
                .withOutputNames(outputNames)
                .build();
            return (B)this;
        }


        /**
         * Keep only observations of the selected features of interest
         * @param filter Filter to select features of interest
         * @return This builder for chaining
         */
        public B withFois(FoiFilter filter)
        {
            instance.foiFilter = filter;
            return (B)this;
        }


        /**
         * Keep only observations of the selected features of interest
         * @param filter Filter to select features of interest
         * @return This builder for chaining
         */
        public B withFois(FoiFilter.Builder filter)
        {
            return withFois(filter.build());
        }


        /**
         * Keep only observations of the specified features of interests
         * @param foiIDs Internal IDs of one or more fois
         * @return This builder for chaining
         */
        public B withFois(Long... foiIDs)
        {
            instance.foiFilter = new FoiFilter.Builder()
                .withInternalIDs(foiIDs)
                .build();
            return (B)this;
        }


        /**
         * Keep only observations of the specified features of interests
         * @param foiUIDs Unique IDs of one or more fois
         * @return This builder for chaining
         */
        public B withFois(String... foiUIDs)
        {
            instance.foiFilter = new FoiFilter.Builder()
                .withUniqueIDs(foiUIDs)
                .build();
            return (B)this;
        }


        /**
         * Keep only the observations whose key matches the predicate
         * @param keyPredicate The predicate to test the key
         * @return This builder for chaining
         */
        public B withKeyPredicate(Predicate<ObsKey> keyPredicate)
        {
            instance.keyPredicate = keyPredicate;
            return (B)this;
        }


        /**
         * Keep only the observations whose data matches the predicate
         * @param valuePredicate The predicate to test the observation data
         * @return This builder for chaining
         */
        public B withValuePredicate(Predicate<ObsData> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return (B)this;
        }


        /**
         * Sets the maximum number of observations to retrieve
         * @param limit Max observations count
         * @return This builder for chaining
         */
        public B withLimit(int limit)
        {
            instance.limit = limit;
            return (B)this;
        }
    }
}
