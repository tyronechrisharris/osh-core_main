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
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.sensorhub.api.datastore.SpatialFilter.SpatialOp;
import org.sensorhub.utils.ObjectUtils;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Bbox;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


/**
 * <p>
 * Immutable filter object for generic features.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class FeatureFilter implements IFeatureFilter
{
    protected Set<Long> internalIDs;
    protected IdFilter featureUIDs;
    protected RangeFilter<Instant> validTime;
    protected SpatialFilter location;
    protected Predicate<FeatureKey> keyPredicate;
    protected Predicate<IFeature> valuePredicate;
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


    public Set<Long> getInternalIDs()
    {
        return internalIDs;
    }


    public IdFilter getFeatureUIDs()
    {
        return featureUIDs;
    }


    public RangeFilter<Instant> getValidTime()
    {
        return validTime;
    }


    public SpatialFilter getLocationFilter()
    {
        return location;
    }


    public Predicate<FeatureKey> getKeyPredicate()
    {
        return keyPredicate;
    }


    public Predicate<IFeature> getValuePredicate()
    {
        return valuePredicate;
    }


    @Override
    public long getLimit()
    {
        return limit;
    }


    @Override
    public boolean test(IFeature f)
    {
        return (testFeatureUIDs(f) &&
                testValidTime(f) &&
                testLocation(f) &&
                testValuePredicate(f));
    }
    
    
    public boolean testInternalIDs(FeatureKey key)
    {
        return (internalIDs == null ||
                internalIDs.contains(key.internalID));
    }
    
    
    public boolean testFeatureUIDs(IFeature f)
    {
        return (featureUIDs == null ||
                featureUIDs.test(f.getUniqueIdentifier()));
    }
    
    
    public boolean testValidTime(IFeature f)
    {
        return (validTime == null ||
                !(f instanceof ITemporalFeature) ||
                ((ITemporalFeature)f).getValidTime() == null ||
                validTime.test(((ITemporalFeature)f).getValidTime()));
    }
    
    
    public boolean testLocation(IFeature f)
    {
        return (location == null ||
                (f instanceof IGeoFeature &&
                ((IGeoFeature)f).getGeometry() != null && 
                location.test((Geometry)((IGeoFeature)f).getGeometry())));
    }
    
    
    public boolean testKeyPredicate(FeatureKey k)
    {
        return (keyPredicate == null ||
                keyPredicate.test(k));
    }
    
    
    public boolean testValuePredicate(IFeature f)
    {
        return (valuePredicate == null ||
                valuePredicate.test(f));
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T extends Builder> T builder()
    {
        return (T)new Builder(new FeatureFilter());
    }
    
    
    @SuppressWarnings("unchecked")
    public static class Builder<B extends Builder<B, F>, F extends FeatureFilter>
    {
        protected F instance;


        protected Builder(F instance)
        {
            this.instance = instance;
        }
        
        
        public B withInternalIDs(long... ids)
        {
            instance.internalIDs = new TreeSet<Long>();
            for (long id: ids)
                instance.internalIDs.add(id);
            return (B)this;
        }
        
        
        public B withInternalIDs(Iterable<Long> ids)
        {
            instance.internalIDs = new TreeSet<Long>();
            for (long id: ids)
                instance.internalIDs.add(id);
            return (B)this;
        }
        
        
        public B withUniqueIDs(IdFilter uids)
        {
            instance.featureUIDs = uids;
            return (B)this;
        }
        
        
        public B withUniqueIDs(String... uids)
        {
            return withUniqueIDs(Arrays.asList(uids));
        }
        
        
        public B withUniqueIDs(Iterable<String> uids)
        {
            instance.featureUIDs = new IdFilter();
            for (String uid: uids)
                instance.featureUIDs.getIdList().add(uid);
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


        public B withValuePredicate(Predicate<IFeature> valuePredicate)
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
