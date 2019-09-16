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
import org.sensorhub.utils.ObjectUtils;


/**
 * <p>
 * Immutable filter object for observation statistics.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Sep 13, 2019
 */
public class ObsStatsFilter implements IQueryFilter
{
    private ProcedureFilter procFilter;
    private FoiFilter foiFilter;
    private RangeFilter<Instant> resultTime;
    private int numHistogramBins = 0;
    private long limit = Long.MAX_VALUE;
    
    
    /*
     * this class can only be instantiated using builder
     */
    private ObsStatsFilter() {}


    public ProcedureFilter getProcedureFilter()
    {
        return procFilter;
    }


    public FoiFilter getFeaturesOfInterestFilter()
    {
        return foiFilter;
    }


    public RangeFilter<Instant> getResultTime()
    {
        return resultTime;
    }


    @Override
    public long getLimit()
    {
        return limit;
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
        private ObsStatsFilter instance = new ObsStatsFilter();


        public Builder withProcedures(ProcedureFilter procFilter)
        {
            instance.procFilter = procFilter;
            return this;
        }


        public Builder withFeaturesOfInterest(FoiFilter foiFilter)
        {
            instance.foiFilter = foiFilter;
            return this;
        }
        
        
        public Builder withResultTimeRange(Instant begin, Instant end)
        {
            instance.resultTime = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return this;
        }


        public Builder withLimit(int limit)
        {
            instance.limit = limit;
            return this;
        }
        
        
        public ObsStatsFilter build()
        {
            return instance;
        }
    }
}
