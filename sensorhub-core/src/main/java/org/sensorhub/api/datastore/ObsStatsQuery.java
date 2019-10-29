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
import org.vast.util.BaseBuilder;


/**
 * <p>
 * Immutable filter object for observation statistics.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Sep 13, 2019
 */
public class ObsStatsQuery implements IQueryFilter
{
    protected DataStreamFilter dataStreamFilter;
    protected FoiFilter foiFilter;
    protected TemporalFilter resultTime;
    protected int numHistogramBins = 0;
    protected boolean aggregateFois = true;
    protected long limit = Long.MAX_VALUE;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected ObsStatsQuery() {}


    public DataStreamFilter getDataStreamFilter()
    {
        return dataStreamFilter;
    }


    public FoiFilter getFoiFilter()
    {
        return foiFilter;
    }


    public TemporalFilter getResultTime()
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
        return ObjectUtils.toString(this, true, true);
    }
    
    
    public static class Builder extends ObsStatsQueryBuilder<Builder, ObsStatsQuery>
    {
        public Builder()
        {
            this.instance = new ObsStatsQuery();
        }
        
        public static Builder from(ObsStatsQuery base)
        {
            return new Builder().copyFrom(base);
        }
    }


    @SuppressWarnings("unchecked")
    public static class ObsStatsQueryBuilder<
            B extends ObsStatsQueryBuilder<B, Q>,
            Q extends ObsStatsQuery>
        extends BaseBuilder<Q>
    {
        
        protected ObsStatsQueryBuilder()
        {
        }
        
        
        protected B copyFrom(ObsStatsQuery base)
        {
            instance.dataStreamFilter = base.dataStreamFilter;
            instance.foiFilter = base.foiFilter;
            instance.resultTime = base.resultTime;
            instance.numHistogramBins = base.numHistogramBins;
            instance.aggregateFois = base.aggregateFois;
            instance.limit = base.limit;
            return (B)this;
        }
        

        public B withDataStreams(DataStreamFilter filter)
        {
            instance.dataStreamFilter = filter;
            return (B)this;
        }
        

        public B withDataStreams(DataStreamFilter.Builder filter)
        {
            return withDataStreams(filter.build());
        }


        public B withDataStreams(Long... dsIDs)
        {
            instance.dataStreamFilter = new DataStreamFilter.Builder()
                .withInternalIDs(dsIDs)
                .build();
            return (B)this;
        }


        public B withFois(FoiFilter filter)
        {
            instance.foiFilter = filter;
            return (B)this;
        }


        public B withFois(FoiFilter.Builder filter)
        {
            return withFois(filter.build());
        }
        
        
        public B withResultTimeRange(Instant begin, Instant end)
        {
            instance.resultTime = new TemporalFilter.Builder()
                    .withRange(begin, end)
                    .build();
            return (B)this;
        }


        public B withLimit(int limit)
        {
            instance.limit = limit;
            return (B)this;
        }
    }
}
