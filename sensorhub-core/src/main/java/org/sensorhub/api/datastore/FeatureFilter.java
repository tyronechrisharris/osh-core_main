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
import org.sensorhub.api.datastore.SpatialFilter.SpatialOp;
import org.vast.ogc.gml.TemporalFeature;
import org.vast.util.Bbox;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import net.opengis.gml.v32.AbstractFeature;


/**
 * <p>
 * Immutable filter object for generic features.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class FeatureFilter implements IQueryFilter, Predicate<AbstractFeature>
{
    protected IdFilter featureIDs;
    protected RangeFilter<Instant> validTime;
    protected SpatialFilter location;
    protected Predicate<FeatureKey> keyPredicate;
    protected Predicate<AbstractFeature> valuePredicate;
    protected long limit = Long.MAX_VALUE;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected FeatureFilter()
    {
        // defaults to currently valid version of feature
        validTime = RangeFilter.<Instant>builder()
                .withSingleValue(Instant.now())
                .build();
    }


    public IdFilter getFeatureIDs()
    {
        return featureIDs;
    }


    public RangeFilter<Instant> getValidTime()
    {
        return validTime;
    }


    public SpatialFilter getLocation()
    {
        return location;
    }


    public Predicate<FeatureKey> getKeyPredicate()
    {
        return keyPredicate;
    }


    public Predicate<AbstractFeature> getValuePredicate()
    {
        return valuePredicate;
    }


    @Override
    public long getLimit()
    {
        return limit;
    }


    @Override
    public boolean test(AbstractFeature f)
    {
        return (testFeatureIDs(f) &&
                testValidTime(f) &&
                testLocation(f) &&
                testValuePredicate(f));
    }
    
    
    public boolean testFeatureIDs(AbstractFeature f)
    {
        return (featureIDs == null ||
                featureIDs.test(f.getUniqueIdentifier()));
    }
    
    
    public boolean testValidTime(AbstractFeature f)
    {
        return (validTime == null ||
                !(f instanceof TemporalFeature) ||
                validTime.test(((TemporalFeature)f).getValidTime()));
    }
    
    
    public boolean testLocation(AbstractFeature f)
    {
        return (location == null ||
                (f.isSetGeometry() && location.test((Geometry)f.getGeometry())));
    }
    
    
    public boolean testValuePredicate(AbstractFeature f)
    {
        return (valuePredicate == null ||
                valuePredicate.test(f));
    }
        
    
    public static class Builder extends BaseBuilder<Builder, FeatureFilter>
    {
        public Builder()
        {
            super(new FeatureFilter());
        }
    }
    
    
    @SuppressWarnings("unchecked")
    protected static class BaseBuilder<B extends BaseBuilder<B, F>, F extends FeatureFilter>
    {
        protected F instance;


        protected BaseBuilder(F instance)
        {
            this.instance = instance;
        }
        
        
        public B withIds(IdFilter ids)
        {
            instance.featureIDs = ids;
            return (B)this;
        }
        
        
        public B withIds(String... ids)
        {
            instance.featureIDs = new IdFilter();
            for (String id: ids)
                instance.featureIDs.getIdList().add(id);
            return (B)this;
        }
        
        
        public B withValidTimeDuring(Instant begin, Instant end)
        {
            instance.validTime = RangeFilter.<Instant>builder()
                    .withRange(begin, end)
                    .build();
            return (B)this;
        }


        public B validAtTime(Instant time)
        {
            instance.validTime = RangeFilter.<Instant>builder()
                    .withSingleValue(time)
                    .build();
            return (B)this;
        }


        public B withLocation(SpatialFilter location)
        {
            instance.location = location;
            return (B)this;
        }


        public B withLocationIntersecting(Polygon roi)
        {
            instance.location = new SpatialFilter.Builder()
                    .withRoi(roi)
                    .build();
            return (B)this;
        }


        public B withLocationWithin(Polygon roi)
        {
            instance.location = new SpatialFilter.Builder()
                    .withRoi(roi)
                    .withOperator(SpatialOp.CONTAINS)
                    .build();
            return (B)this;
        }


        public B withLocationWithin(Bbox bbox)
        {
            instance.location = new SpatialFilter.Builder()
                    .withBbox(bbox)
                    .build();
            return (B)this;
        }


        public B withLocationWithin(Point center, double dist)
        {
            instance.location = new SpatialFilter.Builder()
                    .withDistanceToPoint(center, dist)
                    .build();
            return (B)this;
        }


        public B withKeyPredicate(Predicate<FeatureKey> keyPredicate)
        {
            instance.keyPredicate = keyPredicate;
            return (B)this;
        }


        public B withValuePredicate(Predicate<AbstractFeature> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return (B)this;
        }
        
        
        public B withLimit(int limit)
        {
            instance.limit = limit;
            return (B)this;
        }


        public F build()
        {
            F newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
