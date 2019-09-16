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
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import com.google.common.collect.Range;


/**
 * <p>
 * Immutable object representing statistics for a bucket of observation.</br>
 * There can be only one procedure and one FOI attached to each bucket.
 * </p>
 *
 * @author Alex Robin
 * @since Sep 13, 2019
 */
public class ObsStats
{    
    private long procedureID = 0;
    private long foiID = 0;
    private long obsCount = 0;
    private Range<Instant> resultTimeRange = null;
    private Range<Instant> phenomenonTimeRange = null;
    private Bbox phenomenonBbox = null;
    private int[] obsCountsByTime = null;
    
    
    /*
     * this class can only be instantiated using builder
     */
    ObsStats()
    {
    }


    /**
     * @return The unique ID of the procedure that produced the observations 
     * in this bucket.
     */
    public long getProcedureID()
    {
        return procedureID;
    }


    /**
     * @return The unique ID of the feature of interest targeted by observations
     * in this bucket.
     */
    public long getFoiID()
    {
        return foiID;
    }


    /**
     * @return The number of observations in the series.
     */
    public long getTotalCount()
    {
        return obsCount;
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
    public Range<Instant> getResultTimeRange()
    {
        return resultTimeRange;
    }


    /**
     * @return The range of phenomenon times in this series
     */
    public Range<Instant> getPhenomenonTimeRange()
    {
        return phenomenonTimeRange;
    }


    /**
     * @return Bounding rectangle of phenomenon locations in this series
     */
    public Bbox getPhenomenonBbox()
    {
        return phenomenonBbox;
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
        private ObsStats instance = new ObsStats();


        Builder withProcedureID(long procedureID)
        {
            instance.procedureID = procedureID;
            return this;
        }


        public Builder withFoiID(long foiID)
        {
            instance.foiID = foiID;
            return this;
        }


        public Builder withPhenomenonTimeRange(Range<Instant> timeRange)
        {
            instance.phenomenonTimeRange = timeRange;
            return this;
        }


        public Builder withResultTimeRange(Range<Instant> timeRange)
        {
            instance.resultTimeRange = timeRange;
            return this;
        }


        public Builder withPhenomenonBbox(Bbox bbox)
        {
            instance.phenomenonBbox = bbox;
            return this;
        }
        
        
        public ObsStats build()
        {
            Asserts.checkNotNull(instance.procedureID, "procedureID");
            Asserts.checkState(instance.phenomenonTimeRange != null || instance.resultTimeRange != null, "At least one time range must be set");
            return instance;
        }
    }
}
