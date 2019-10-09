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
import org.vast.util.BaseBuilder;


/**
 * <p>
 * Immutable filter object for observation periods.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2018
 */
public class ObsClusterFilter implements IQueryFilter
{
    private RangeFilter<Instant> timeRange;
    private SpatialFilter phenomenonLocation;
    private ProcedureFilter procedures;
    private FoiFilter featuresOfInterest;
    private int limit;
    
    
    /*
     * this class can only be instantiated using builder
     */
    private ObsClusterFilter() {}
    
    
    /**
     * @return Time range limiting the returned clusters. Only clusters
     * intersecting this time range will be returned
     */
    public RangeFilter<Instant> getTimeRange()
    {
        return timeRange;
    }


    /**
     * @return Bounding rectangle of all returned clusters
     */
    public SpatialFilter getPhenomenonLocation()
    {
        return phenomenonLocation;
    }


    /**
     * @return Filter criteria to select procedures included in results.
     * If set, separate clusters are returned for each selected procedure.
     * If null, clusters contain observations from all available procedures. 
     */
    public ProcedureFilter getProcedures()
    {
        return procedures;
    }


    public FoiFilter getFeaturesOfInterest()
    {
        return featuresOfInterest;
    }


    @Override
    public long getLimit()
    {
        return limit;
    }
    
    
    public static Builder builder()
    {
        return new Builder();
    }


    public static class Builder extends BaseBuilder<ObsClusterFilter>
    {
        protected Builder()
        {
            super(new ObsClusterFilter());
        }


        public Builder withTimeRange(Instant begin, Instant end)
        {
            instance.timeRange = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return this;
        }


        public Builder withSpatialFilter(SpatialFilter phenomenonLocation)
        {
            instance.phenomenonLocation = phenomenonLocation;
            return this;
        }


        public Builder withProcedures(ProcedureFilter procedures)
        {
            instance.procedures = procedures;
            return this;
        }


        public Builder withFeaturesOfInterest(FoiFilter featuresOfInterest)
        {
            instance.featuresOfInterest = featuresOfInterest;
            return this;
        }


        public Builder withLimit(int limit)
        {
            instance.limit = limit;
            return this;
        }
    }
}
