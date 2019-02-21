/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.

 Contributor(s): 
    Alexandre Robin "alex.robin@sensiasoft.com"
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.time.Instant;


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
    
    
    public static ObsPeriodFilterBuilder builder()
    {
        return new ObsPeriodFilterBuilder();
    }


    public static class ObsPeriodFilterBuilder
    {
        private ObsClusterFilter instance = new ObsClusterFilter();


        public ObsPeriodFilterBuilder withTimeRange(Instant begin, Instant end)
        {
            instance.timeRange = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return this;
        }


        public ObsPeriodFilterBuilder withSpatialFilter(SpatialFilter phenomenonLocation)
        {
            instance.phenomenonLocation = phenomenonLocation;
            return this;
        }


        public ObsPeriodFilterBuilder withProcedures(ProcedureFilter procedures)
        {
            instance.procedures = procedures;
            return this;
        }


        public ObsPeriodFilterBuilder withFeaturesOfInterest(FoiFilter featuresOfInterest)
        {
            instance.featuresOfInterest = featuresOfInterest;
            return this;
        }


        public ObsPeriodFilterBuilder withLimit(int limit)
        {
            instance.limit = limit;
            return this;
        }
        
        
        public ObsClusterFilter build()
        {
            return instance;
        }
    }
}
