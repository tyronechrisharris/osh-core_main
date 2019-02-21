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
import java.util.function.Predicate;
import org.sensorhub.api.persistence.ObsKey;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Immutable filter object for observations.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Jan 16, 2018
 */
public class ObsFilter implements IQueryFilter
{
    private RangeFilter<Instant> phenomenonTime;
    private RangeFilter<Instant> resultTime;
    private SpatialFilter phenomenonLocation;
    private ProcedureFilter procedures;
    private FoiFilter featuresOfInterest;
    private Predicate<ObsKey> keyPredicate;
    private Predicate<ObsData> valuePredicate;
    private Predicate<DataBlock> resultPredicate;
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


    public ProcedureFilter getProcedures()
    {
        return procedures;
    }


    public FoiFilter getFeaturesOfInterest()
    {
        return featuresOfInterest;
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


    public static class Builder
    {
        private ObsFilter instance = new ObsFilter();


        public Builder withPhenomenonTimeRange(Instant begin, Instant end)
        {
            instance.phenomenonTime = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return this;
        }


        public Builder withResultTimeRange(Instant begin, Instant end)
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
