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

import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
import org.vast.util.Bbox;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Immutable object representing statistics for a bucket of observations.</br>
 * There can be only one data stream and one FOI attached to each bucket.
 * </p>
 *
 * @author Alex Robin
 * @since Sep 13, 2019
 */
public class ObsStats
{
    protected Long dataStreamID;
    protected FeatureId foiID;
    protected TimeExtent phenomenonTimeRange;
    protected TimeExtent resultTimeRange;
    protected Bbox phenomenonBbox = null;
    protected long totalObsCount = 0;
    protected int[] obsCountsByTime = null;
    
    
    /*
     * this class can only be instantiated using builder
     */
    ObsStats()
    {
    }


    /**
     * @return The unique ID of the data stream that contains the observations 
     * in this bucket.
     */
    public Long getDataStreamID()
    {
        return dataStreamID;
    }


    /**
     * @return The unique ID of the feature of interest targeted by observations
     * in this bucket.
     */
    public FeatureId getFoiID()
    {
        return foiID;
    }


    /**
     * @return The range of phenomenon times in this series
     */
    public TimeExtent getPhenomenonTimeRange()
    {
        return phenomenonTimeRange;
    }


    /**
     * @return The range of result times in this observation series. There are
     * 3 main cases:<br/>
     * <li>The returned range is equal to the phenomenon time range for many
     * sensors for which sampling and measurement operations are considered
     * to be simultaneous (most electronic sensors fit into this category).</li>
     * <li>The returned range is different from the phenomenon time range when
     * the measured phenomenon happened in the past or is forecasted to happen in
     * the future.</li>
     * <li>A time instant is returned if all observation in the series share
     * the same result time (e.g. model run).</li>
     */
    public TimeExtent getResultTimeRange()
    {
        if (resultTimeRange == null)
            return phenomenonTimeRange;
        return resultTimeRange;
    }


    /**
     * @return Bounding rectangle of phenomenon locations in this series
     */
    public Bbox getPhenomenonBbox()
    {
        return phenomenonBbox;
    }


    /**
     * @return The total number of observations in the series.
     */
    public long getTotalObsCount()
    {
        return totalObsCount;
    }
    

    /**
     * @return The histogram of observations count vs. time
     */
    public int[] getObsCountsByTime()
    {
        return obsCountsByTime;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends ObsStatsBuilder<Builder, ObsStats>
    {
        public Builder()
        {
            this.instance = new ObsStats();
        }
        
        public static Builder from(ObsStats base)
        {
            return new Builder().copyFrom(base);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class ObsStatsBuilder<
            B extends ObsStatsBuilder<B, T>,
            T extends ObsStats>
        extends BaseBuilder<T>
    {       
        protected ObsStatsBuilder()
        {
        }
        
        
        protected B copyFrom(ObsStats base)
        {
            instance.dataStreamID = base.dataStreamID;
            instance.foiID = base.foiID;
            instance.totalObsCount = base.totalObsCount;
            instance.phenomenonTimeRange = base.phenomenonTimeRange;
            instance.resultTimeRange = base.resultTimeRange;
            instance.phenomenonBbox = base.phenomenonBbox;
            instance.obsCountsByTime = base.obsCountsByTime;
            return (B)this;
        }

        
        public B withDataStreamID(long dataStreamID)
        {
            instance.dataStreamID = dataStreamID;
            return (B)this;
        }


        public B withFoiID(FeatureId foiID)
        {
            instance.foiID = foiID;
            return (B)this;
        }


        public B withPhenomenonTimeRange(TimeExtent timeRange)
        {
            instance.phenomenonTimeRange = timeRange;
            return (B)this;
        }


        public B withResultTimeRange(TimeExtent timeRange)
        {
            instance.resultTimeRange = timeRange;
            return (B)this;
        }


        public B withPhenomenonBbox(Bbox bbox)
        {
            instance.phenomenonBbox = bbox;
            return (B)this;
        }


        public B withTotalObsCount(long count)
        {
            instance.totalObsCount = count;
            return (B)this;
        }


        public B withObsCountByTime(int[] counts)
        {
            instance.obsCountsByTime = counts;
            return (B)this;
        }
        
        
        public T build()
        {
            Asserts.checkArgument(instance.dataStreamID != null && instance.dataStreamID > 0, "dataStreamID must be > 0");
            Asserts.checkState(instance.phenomenonTimeRange != null || instance.resultTimeRange != null, "At least one time range must be set");
            return super.build();
        }
    }
}
