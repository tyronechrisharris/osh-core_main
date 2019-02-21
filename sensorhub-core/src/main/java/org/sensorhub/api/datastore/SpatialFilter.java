/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.

 Contributor(s): 
    Alexandre Robin "alex.robin@sensiasoft.com"
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.function.Predicate;
import org.vast.util.BaseBuilder;
import org.vast.util.Bbox;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;


/**
 * <p>
 * Immutable spatial filter definition
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class SpatialFilter implements Predicate<Geometry>
{
    public enum SpatialOp
    {
        INTERSECTS, CONTAINS, EQUALS, WITHIN, DISJOINT, DISTANCE
    }

    protected SpatialOp operator;
    protected Geometry roi;
    protected Point center;
    protected double distance = Double.NaN;
    
    protected transient PreparedGeometry preparedGeom;
    protected transient Predicate<Geometry> geomTest;


    /*
     * this class can only be instantiated using builder
     */
    protected SpatialFilter()
    {        
    }
    
    
    public Geometry getRoi()
    {
        return roi;
    }


    public SpatialOp getOperator()
    {
        return operator;
    }


    @Override
    public boolean test(Geometry geom)
    {
        return geomTest.test(geom);
    }
    
    
    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append(operator).append(' ');
        if (!Double.isNaN(distance))
        {
            buf.append(distance)
               .append(" FROM ")
               .append(center);
        }
        else
            buf.append(roi);
        return buf.toString();
    }


    public static class Builder extends SpatialFilterBuilder<Builder, SpatialFilter>
    {
        public Builder()
        {
            super(new SpatialFilter());
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static class SpatialFilterBuilder<
            B extends SpatialFilterBuilder<B, T>,
            T extends SpatialFilter>
        extends BaseBuilder<B, T>
    {
        
        protected SpatialFilterBuilder(T instance)
        {
            super(instance);
        }
        
        
        public B withRoi(Geometry geom)
        {
            instance.roi = geom;
            instance.preparedGeom = PreparedGeometryFactory.prepare(geom);
            return (B)this;
        }
        
        
        public B withDistanceToPoint(Point center, double distance)
        {
            instance.center = center;
            instance.distance = distance;
            withRoi(center.buffer(distance, 1).getEnvelope());
            withOperator(SpatialOp.DISTANCE);
            return (B)this;
        }
        
        
        public B withBbox(Bbox bbox)
        {
            withRoi(bbox.toJtsPolygon());
            withOperator(SpatialOp.INTERSECTS);
            return (B)this;
        }
        
        
        public B withOperator(SpatialOp op)
        {
            instance.operator = op;
            final T instanceLocal = instance;
            
            switch (op)
            {
                case INTERSECTS:
                    instance.geomTest = (g -> instanceLocal.preparedGeom.intersects(g));
                    break;
                    
                case CONTAINS:
                    instance.geomTest = (g -> instanceLocal.preparedGeom.contains(g));
                    break;
                    
                case EQUALS:
                    instance.geomTest = (g -> instanceLocal.preparedGeom.equals(g));
                    break;
                    
                case WITHIN:
                    instance.geomTest = (g -> instanceLocal.preparedGeom.within(g));
                    break;
                    
                case DISJOINT:
                    instance.geomTest = (g -> instanceLocal.preparedGeom.disjoint(g));
                    break;
                    
                case DISTANCE:
                    instance.geomTest = (g -> instanceLocal.center.isWithinDistance(g, instanceLocal.distance));
                    break;
                    
                default:
                    throw new IllegalArgumentException("Unsupported operator " + op);
            }
            
            return (B)this;
        }
        
        
        @Override
        public T build()
        {
            if (instance.operator == null)
                withOperator(SpatialOp.INTERSECTS);
            
            if (instance.operator == SpatialOp.DISTANCE && Double.isNaN(instance.distance))
                throw new IllegalStateException(SpatialOp.DISTANCE + " operator must be set along with a distance value");
                   
            return super.build();
        }
    }
}
