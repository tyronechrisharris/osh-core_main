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
import org.vast.util.Asserts;
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
    protected RangeFilter<Instant> phenomenonTime;
    protected RangeFilter<Instant> resultTime;
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
    
    
    public RangeFilter<Instant> getPhenomenonTime()
    {
        return phenomenonTime;
    }


    public RangeFilter<Instant> getResultTime()
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
    public static Builder builder()
    {
        return new Builder();
    }


    public static class Builder extends BaseBuilder<ObsFilter>
    {
        protected Builder()
        {
            super(new ObsFilter());
        }
        
        
        public Builder from(ObsFilter base)
        {
            instance.phenomenonTime = base.phenomenonTime;
            instance.resultTime = base.resultTime;
            instance.phenomenonLocation = base.phenomenonLocation;
            instance.dataStreams = base.dataStreams;
            instance.foiFilter = base.foiFilter;
            instance.keyPredicate = base.keyPredicate;
            instance.valuePredicate = base.valuePredicate;
            instance.limit = base.limit;
            return this;
        }


        /**
         * Keep only observations whose phenomenon time is within the given period.<br/>
         * The phenomenon time is the time when the phenomenon being measured happened.
         * @param begin Beginning of desired period
         * @param end End of desired period
         * @return This builder for chaining
         */
        public Builder withPhenomenonTimeDuring(Instant begin, Instant end)
        {
            instance.phenomenonTime = RangeFilter.<Instant>builder()
                .withRange(begin, end)
                .build();
            return this;
        }


        /**
         * Keep only observations whose result time is within the given period.<br/>
         * The result time is the time when the data resulting from the observation
         * was generated.
         * @param begin Beginning of desired period
         * @param end End of desired period
         * @return This builder for chaining
         */
        public Builder withResultTimeDuring(Instant begin, Instant end)
        {
            instance.resultTime = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return this;
        }
        
        
        /**
         * Keep only observation(s) with the latest result time.<br/>
         * For many sensors, this means only the last observation collected.<br/>
         * For models, this means observations generated during the latest run.
         * @return This builder for chaining
         */
        public Builder withLatestResult()
        {
            instance.resultTime = RangeFilter.<Instant>builder()
                .withSingleValue(Instant.MAX).build();
            return this;
        }


        /**
         * Keep only observations whose phenomenon location, if any, matches the spatial filter.
         * @param filter Spatial filtering options
         * @return This builder for chaining
         */
        public Builder withPhenomenonLocation(SpatialFilter filter)
        {
            checkSpatialFilter();
            instance.phenomenonLocation = filter;
            return this;
        }


        /**
         * Keep only observations from data streams matching the filter.
         * @param filter Filter to select desired data streams
         * @return This builder for chaining
         */
        public Builder withDataStreams(DataStreamFilter filter)
        {
            checkDataStreamFilter();
            instance.dataStreams = filter;
            return this;
        }


        /**
         * Keep only observations from specific data streams.
         * @param ids Internal IDs of one or more data streams
         * @return This builder for chaining
         */
        public Builder withDataStreams(Long... ids)
        {
            checkDataStreamFilter();
            instance.dataStreams = DataStreamFilter.builder()
                .withInternalIDs(ids)
                .build();
            return this;
        }


        /**
         * Keep only observations from specific procedures (including all outputs).
         * @param procIDs Internal IDs of one or more procedures
         * @return This builder for chaining
         */
        public Builder withProcedures(Long... procIDs)
        {
            checkDataStreamFilter();
            instance.dataStreams = DataStreamFilter.builder()
                .withProcedures(procIDs)
                .build();
            return this;
        }


        /**
         * Keep only observations produced by certain outputs of a specific procedure
         * @param procID Internal ID of the procedure
         * @param outputNames Names of one or more outputs of interest
         * @return This builder for chaining
         */
        public Builder withProcedure(long procID, String... outputNames)
        {
            checkDataStreamFilter();
            instance.dataStreams = DataStreamFilter.builder()
                .withProcedures(procID)
                .withOutputNames(outputNames)
                .build();
            return this;
        }


        /**
         * Keep only observations of the selected features of interest
         * @param filter Filter to select features of interest
         * @return This builder for chaining
         */
        public Builder withFois(FoiFilter filter)
        {
            checkFoiFilter();
            instance.foiFilter = filter;
            return this;
        }


        /**
         * Keep only observations of the specified features of interests
         * @param foiIDs Internal IDs of one or more fois
         * @return This builder for chaining
         */
        public Builder withFois(Long... foiIDs)
        {
            checkFoiFilter();
            instance.foiFilter = FoiFilter.builder()
                .withInternalIDs(foiIDs)
                .build();
            return this;
        }


        /**
         * Keep only observations of the specified features of interests
         * @param foiUIDs Unique IDs of one or more fois
         * @return This builder for chaining
         */
        public Builder withFois(String... foiUIDs)
        {
            checkFoiFilter();
            instance.foiFilter = FoiFilter.builder()
                .withUniqueIDs(foiUIDs)
                .build();
            return this;
        }


        /**
         * Keep only the observations whose key matches the predicate
         * @param keyPredicate The predicate to test the key
         * @return This builder for chaining
         */
        public Builder withKeyPredicate(Predicate<ObsKey> keyPredicate)
        {
            instance.keyPredicate = keyPredicate;
            return this;
        }


        /**
         * Keep only the observations whose data matches the predicate
         * @param valuePredicate The predicate to test the observation data
         * @return This builder for chaining
         */
        public Builder withValuePredicate(Predicate<ObsData> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return this;
        }


        /**
         * Sets the maximum number of observations to retrieve
         * @param limit Max observations count
         * @return This builder for chaining
         */
        public Builder withLimit(int limit)
        {
            instance.limit = limit;
            return this;
        }
        
        
        protected void checkSpatialFilter()
        {
            Asserts.checkState(instance.phenomenonLocation == null, "spatial filter already configured");
        }
        
        
        protected void checkDataStreamFilter()
        {
            Asserts.checkState(instance.dataStreams == null, "datastream filter already configured");
        }
        
        
        protected void checkFoiFilter()
        {
            Asserts.checkState(instance.foiFilter == null, "foi filter already configured");
        }
    }
}
