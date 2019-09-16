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


/**
 * <p>
 * Immutable filter object for observations.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Jan 16, 2018
 */
public class ObsFilter implements IQueryFilter, Predicate<ObsData>
{
    private RangeFilter<Instant> phenomenonTime;
    private RangeFilter<Instant> resultTime;
    private SpatialFilter phenomenonLocation;
    private ProcedureFilter procFilter;
    private FoiFilter foiFilter;
    private Predicate<ObsKey> keyPredicate;
    private Predicate<ObsData> valuePredicate;
    private long limit = Long.MAX_VALUE;
    
    
    /*
     * this class can only be instantiated using builder
     */
    private ObsFilter() {}
    
    
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


    public ProcedureFilter getProcedureFilter()
    {
        return procFilter;
    }


    public FoiFilter getFeatureOfInterestFilter()
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
        return ObjectUtils.toString(this, true);
    }
    
    
    public static Builder builder()
    {
        return new Builder();
    }


    public static class Builder
    {
        private ObsFilter instance = new ObsFilter();


        public Builder withPhenomenonTimeDuring(Instant begin, Instant end)
        {
            instance.phenomenonTime = RangeFilter.<Instant>builder()
                .withRange(begin, end)
                .build();
            return this;
        }


        public Builder withResultTimeDuring(Instant begin, Instant end)
        {
            instance.resultTime = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return this;
        }


        public Builder withPhenomenonLocation(SpatialFilter phenomenonLocation)
        {
            instance.phenomenonLocation = phenomenonLocation;
            return this;
        }


        public Builder withProcedures(ProcedureFilter procFilter)
        {
            instance.procFilter = procFilter;
            return this;
        }


        public Builder withProcedures(long... procIDs)
        {
            instance.procFilter = ProcedureFilter.builder()
                .withInternalIDs(procIDs)
                .build();
            return this;
        }


        public Builder withProcedures(String... procUIDs)
        {
            instance.procFilter = ProcedureFilter.builder()
                .withUniqueIDs(procUIDs)
                .build();
            return this;
        }


        public Builder withFeaturesOfInterest(FoiFilter foiFilter)
        {
            instance.foiFilter = foiFilter;
            return this;
        }


        public Builder withFeaturesOfInterest(long... foiIDs)
        {
            instance.foiFilter = FoiFilter.builder()
                .withInternalIDs(foiIDs)
                .build();
            return this;
        }


        public Builder withFeaturesOfInterest(String... foiUIDs)
        {
            instance.foiFilter = FoiFilter.builder()
                .withUniqueIDs(foiUIDs)
                .build();
            return this;
        }


        public Builder withKeyPredicate(Predicate<ObsKey> keyPredicate)
        {
            instance.keyPredicate = keyPredicate;
            return this;
        }


        public Builder withValuePredicate(Predicate<ObsData> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return this;
        }


        public Builder withLimit(int limit)
        {
            instance.limit = limit;
            return this;
        }
        
        
        public ObsFilter build()
        {
            return instance;
        }
    }
}
