/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.feature;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.sensorhub.api.datastore.RangeOrSet;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.SpatialFilter.SpatialOp;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.resource.ResourceFilter;
import org.sensorhub.utils.ObjectUtils;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


/**
 * <p>
 * Base class for feature filters.<br/>
 * There is an implicit AND between all filter parameters.
 * </p>
 *
 * @param <T> Type of resource supported by this filter
 * 
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public abstract class FeatureFilterBase<T extends IFeature> extends ResourceFilter<T>
{
    protected static final Instant LATEST_VERSION = Instant.MAX;
        
    protected RangeOrSet<String> featureUIDs;
    protected TemporalFilter validTime;
    protected SpatialFilter location;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected FeatureFilterBase()
    {
        // defaults to currently valid version of feature
        validTime = new TemporalFilter.Builder()
            .withCurrentTime()
            .build();
    }


    public RangeOrSet<String> getFeatureUIDs()
    {
        return featureUIDs;
    }


    public TemporalFilter getValidTime()
    {
        return validTime;
    }


    public SpatialFilter getLocationFilter()
    {
        return location;
    }


    @Override
    public boolean test(T f)
    {
        return (super.test(f) &&
                testFeatureUIDs(f) &&
                testValidTime(f) &&
                testLocation(f));
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
    
    
    protected <B extends FeatureFilterBuilder<?,T,?>> B and(FeatureFilterBase<T> otherFilter, B builder) throws EmptyFilterIntersection
    {
        super.and(otherFilter, builder);
        
        var featureUIDs = this.featureUIDs != null ? this.featureUIDs.and(otherFilter.featureUIDs) : otherFilter.featureUIDs;
        if (featureUIDs != null)
            builder.withUniqueIDs(featureUIDs.getSet());
        
        var validTime = this.validTime != null ? this.validTime.and(otherFilter.validTime) : otherFilter.validTime;
        if (validTime != null)
            builder.withValidTime(validTime);
        
        var location = this.location != null ? this.location.and(otherFilter.location) : otherFilter.location;
        if (location != null)
            builder.withLocation(location);
        
        return builder;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true, true);
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class FeatureFilterBuilder<
            B extends FeatureFilterBuilder<B, V, F>,
            V extends IFeature,
            F extends FeatureFilterBase<V>>
        extends ResourceFilterBuilder<B, V, F>
    {        
        
        protected FeatureFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        protected B copyFrom(F base)
        {
            Asserts.checkNotNull(base, FeatureFilterBase.class);
            super.copyFrom(base);
            instance.featureUIDs = base.getFeatureUIDs();
            instance.validTime = base.getValidTime();
            instance.location = base.getLocationFilter();
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
            instance.featureUIDs = RangeOrSet.from(uids);            
            return (B)this;
        }
        
        
        /**
         * Keep only features with unique IDs starting with given prefix.
         * @param prefix UID prefix
         * @return This builder for chaining
         */
        public B withUniqueIDPrefix(String prefix)
        {
            String begin = prefix + Character.toString((char)0);
            String end = prefix + Character.toString((char)Integer.MAX_VALUE);
            instance.featureUIDs = RangeOrSet.from(begin, end);            
            return (B)this;
        }


        /**
         * Keep only features whose temporal validity matches the filter.
         * @param timeFilter Temporal filter (see {@link TemporalFilter})
         * @return This builder for chaining
         */
        public B withValidTime(TemporalFilter timeFilter)
        {
            instance.validTime = timeFilter;
            return (B)this;
        }
        
        
        /**
         * Keep only features that are valid at some point during the specified period.
         * @param begin Beginning of search period
         * @param end End of search period
         * @return This builder for chaining
         */
        public B withValidTimeDuring(Instant begin, Instant end)
        {
            instance.validTime = new TemporalFilter.Builder()
                    .withRange(begin, end)
                    .build();
            return (B)this;
        }


        /**
         * Keep only feature representations that are valid at the specified time.
         * @param time Time instant of interest (can be set to past or future)
         * @return This builder for chaining
         */
        public B validAtTime(Instant time)
        {
            instance.validTime = new TemporalFilter.Builder()
                    .withSingleValue(time)
                    .build();
            return (B)this;
        }


        /**
         * Keep only feature representations that are valid at the current time.
         * @return This builder for chaining
         */
        public B validNow()
        {
            instance.validTime = new TemporalFilter.Builder()
                .withCurrentTime()
                .build();
            return (B)this;
        }
        
        
        /**
         * Keep only the latest version of features.
         * @return This builder for chaining
         */
        public B withLatestVersion()
        {
            instance.validTime = new TemporalFilter.Builder()
                .withLatestTime()
                .build();
            return (B)this;
        }
        
        
        /**
         * Keep all versions of each selected feature.
         * @return This builder for chaining
         */
        public B withAllVersions()
        {
            instance.validTime = new TemporalFilter.Builder()
                .withAllTimes()
                .build();
            return (B)this;
        }

        
        /**
         * Keep only features whose geometry matches the filter.
         * @return The {@link SpatialFilter} builder for chaining
         */
        public SpatialFilter.NestedBuilder<B> withLocation()
        {
            return new SpatialFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    FeatureFilterBuilder.this.instance.location = build();
                    return (B)FeatureFilterBuilder.this;
                }                
            };
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
    }
}
