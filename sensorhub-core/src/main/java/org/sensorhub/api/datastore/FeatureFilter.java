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
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.sensorhub.api.datastore.SpatialFilter.SpatialOp;
import org.sensorhub.utils.ObjectUtils;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;
import org.vast.util.Bbox;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


/**
 * <p>
 * Immutable filter object for generic features.<br/>
 * There is an implicit AND between all filter parameters.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class FeatureFilter implements IFeatureFilter
{
    protected SortedSet<Long> internalIDs;
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
        validTime = new RangeFilter.Builder<Instant>()
                .withSingleValue(Instant.now())
                .build();
    }


    public SortedSet<Long> getInternalIDs()
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
        return ObjectUtils.toString(this, true, true);
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends FeatureFilterBuilder<Builder, FeatureFilter>
    {
        public Builder()
        {
            super(new FeatureFilter());
        }
        
        protected Builder(FeatureFilter instance)
        {
            this.instance = instance;
        }
        
        public static Builder from(FeatureFilter base)
        {
            return new Builder(null).copyFrom(base);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class FeatureFilterBuilder<
            B extends FeatureFilterBuilder<B, F>,
            F extends FeatureFilter>
        extends BaseBuilder<F>
    {        
        protected FeatureFilterBuilder()
        {
        }
        
        
        protected FeatureFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        protected B copyFrom(FeatureFilter base)
        {
            Asserts.checkNotNull(base, FeatureFilter.class);
            instance.internalIDs = base.internalIDs;
            instance.featureUIDs = base.featureUIDs;
            instance.validTime = base.validTime;
            instance.location = base.location;
            instance.keyPredicate = base.keyPredicate;
            instance.valuePredicate = base.valuePredicate;
            instance.limit = base.limit;
            return (B)this;
        }
        
        
        /**
         * Keep only features with specific internal IDs.
         * @param ids One or more internal IDs of features to select
         * @return This builder for chaining
         */
        public B withInternalIDs(Long... ids)
        {
            return withInternalIDs(Arrays.asList(ids));
        }
        
        
        /**
         * Keep only features with specific internal IDs.
         * @param ids Collection of internal IDs
         * @return This builder for chaining
         */
        public B withInternalIDs(Collection<Long> ids)
        {
            instance.internalIDs = new TreeSet<Long>();            
            for (Long id: ids)
                instance.internalIDs.add(id);            
            return (B)this;
        }
        
        
        /**
         * Keep only features with specific unique IDs.
         * @param uids One or more unique IDs of features to select
         * @return This builder for chaining
         */
        public B withUniqueIDs(String... uids)
        {
            return withUniqueIDs(Arrays.asList(uids));
        }
        
        
        /**
         * Keep only features with specific unique IDs.
         * @param uids Collection of unique IDs
         * @return This builder for chaining
         */
        public B withUniqueIDs(Collection<String> uids)
        {
            instance.featureUIDs = new IdFilter();            
            for (String uid: uids)
                instance.featureUIDs.getIdList().add(uid);            
            return (B)this;
        }
        
        
        /**
         * Keep only feature representations that are valid at any time during the
         * specified period.
         * @param begin Beginning of search period
         * @param end End of search period
         * @return This builder for chaining
         */
        public B withValidTimeDuring(Instant begin, Instant end)
        {
            instance.validTime = new RangeFilter.Builder<Instant>()
                    .withRange(begin, end)
                    .build();
            return (B)this;
        }


        /**
         * Keep only feature representations that are valid at the specified time.
         * @param time Time instant of interest (can be set to past or future, and defaults to 'now')
         * @return This builder for chaining
         */
        public B validAtTime(Instant time)
        {
            instance.validTime = new RangeFilter.Builder<Instant>()
                    .withSingleValue(time)
                    .build();
            return (B)this;
        }
        
        
        /**
         * Keep only the latest version of features.
         * @return This builder for chaining
         */
        public B withLatestVersion()
        {
            instance.validTime = new RangeFilter.Builder<Instant>()
                .withSingleValue(Instant.MAX).build();
            return (B)this;
        }


        /**
         * Keep only features whose geometry matches the filter.
         * @param location Spatial filter (see {@link SpatialFilter})
         * @return This builder for chaining
         */
        public B withLocation(SpatialFilter location)
        {
            instance.location = location;
            return (B)this;
        }


        /**
         * Keep only features whose geometry matches the filter.
         * @param location Spatial filter (see {@link SpatialFilter})
         * @return This builder for chaining
         */
        public B withLocation(SpatialFilter.Builder location)
        {
            return withLocation(location.build());
        }


        /**
         * Keep only features whose geometry intersects the given ROI.
         * @param roi Region of interest expressed as a polygon
         * @return This builder for chaining
         */
        public B withLocationIntersecting(Polygon roi)
        {
            instance.location = new SpatialFilter.Builder()
                    .withRoi(roi)
                    .build();
            return (B)this;
        }


        /**
         * Keep only features whose geometry is contained within the given region.
         * @param roi Region of interest expressed as a polygon
         * @return This builder for chaining
         */
        public B withLocationWithin(Polygon roi)
        {
            instance.location = new SpatialFilter.Builder()
                    .withRoi(roi)
                    .withOperator(SpatialOp.CONTAINS)
                    .build();
            return (B)this;
        }


        /**
         * Keep only features whose geometry is contained within the given region.
         * @param bbox Region of interest expressed as a bounding box
         * @return This builder for chaining
         */
        public B withLocationWithin(Bbox bbox)
        {
            instance.location = new SpatialFilter.Builder()
                    .withBbox(bbox)
                    .build();
            return (B)this;
        }


        /**
         * Keep only features whose geometry is contained within the given region,
         * expressed as a circle (i.e. a distance from a given point).
         * @param center Center of the circular region of interest
         * @param dist Distance from the center = circle radius (in meters)
         * @return This builder for chaining
         */
        public B withLocationWithin(Point center, double dist)
        {
            instance.location = new SpatialFilter.Builder()
                    .withDistanceToPoint(center, dist)
                    .build();
            return (B)this;
        }


        /**
         * Keep only features whose key matches the predicate.
         * @param keyPredicate Predicate to apply to the feature key object
         * @return This builder for chaining
         */
        public B withKeyPredicate(Predicate<FeatureKey> keyPredicate)
        {
            instance.keyPredicate = keyPredicate;
            return (B)this;
        }


        /**
         * Keep only features matching the predicate.
         * @param valuePredicate Predicate to apply to the feature object
         * @return This builder for chaining
         */
        public B withValuePredicate(Predicate<IFeature> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return (B)this;
        }
        
        
        /**
         * Limit the number of selected features to the given number
         * @param limit max number of features to retrieve
         * @return This builder for chaining
         */
        public B withLimit(int limit)
        {
            instance.limit = limit;
            return (B)this;
        }


        @Override
        public F build()
        {
            F newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
