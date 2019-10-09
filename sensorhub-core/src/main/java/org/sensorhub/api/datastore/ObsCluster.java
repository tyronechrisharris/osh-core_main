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
import org.vast.util.BaseBuilder;
import org.vast.util.Bbox;
import com.google.common.collect.Range;


/**
 * <p>
 * Immutable object representing a temporal cluster of observations, using 
 * either phenomenon or result time criteria.<br/>
 * A new cluster is created every time a long enough time gap occurs between
 * observations (e.g. sensor was turned off for a while) or when a new FOI
 * is observed.
 * </p>
 *
 * @author Alex Robin
 * @since Apr 5, 2018
 */
public class ObsCluster
{    
    public static final long ALL_PROCEDURES = Long.MAX_VALUE;
    
    private long procedureID = 0;
    private long foiID = 0;
    private Range<Instant> phenomenonTimeRange = null;
    private Range<Instant> resultTimeRange = null;
    private Bbox phenomenonBbox = null;
    
    
    /*
     * this class can only be instantiated using builder
     */
    ObsCluster()
    {
    }


    /**
     * @return The unique ID of the procedure that produced the observations or
     * the constant {@link #ALL_PROCEDURES} if this cluster represents
     * observations from all procedures.
     */
    public long getProcedureID()
    {
        return procedureID;
    }


    /**
     * @return The unique ID of the feature of interest observed during the 
     * period.
     */
    public long getFoiID()
    {
        return foiID;
    }


    /**
     * @return The range of phenomenon times in this cluster
     */
    public Range<Instant> getPhenomenonTimeRange()
    {
        return phenomenonTimeRange;
    }


    /**
     * @return The range of result times in this cluster
     */
    public Range<Instant> getResultTimeRange()
    {
        return resultTimeRange;
    }


    /**
     * @return Bounding rectangle of phenomenon locations in this cluster
     */
    public Bbox getPhenomenonBbox()
    {
        return phenomenonBbox;
    }
    
    
    public ObsCluster copy()
    {
        ObsCluster newPeriod = new ObsCluster();
        newPeriod.procedureID = this.procedureID;
        newPeriod.foiID = this.foiID;
        newPeriod.phenomenonTimeRange = this.phenomenonTimeRange;
        newPeriod.resultTimeRange = this.resultTimeRange;
        newPeriod.phenomenonBbox = this.phenomenonBbox.copy();
        return newPeriod;
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
    
    
    public static class Builder extends BaseBuilder<ObsCluster>
    {
        protected Builder()
        {
            super(new ObsCluster());
        }


        public Builder withProcedureID(long procedureID)
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
        
        
        public ObsCluster build()
        {
            Asserts.checkNotNull(instance.procedureID, "procedureID");
            Asserts.checkState(instance.phenomenonTimeRange != null || instance.resultTimeRange != null, "At least one time range must be set");
            return super.build();
        }
    }
}
