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
import org.vast.util.Asserts;
import com.google.common.collect.Range;


/**
 * <p>
 * Filter for numerical ranges.<br/>
 * 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 4, 2018
 * @param <K> Type of range bounds
 */
public class RangeFilter<K extends Comparable<?>> implements Predicate<Object>
{    
    private Range<K> range;
    private RangeOp op = RangeOp.INTERSECTS;
    
    
    public enum RangeOp
    {
        INTERSECTS,
        CONTAINS,
        EQUALS        
    }
    
    
    public Range<K> getRange()
    {
        return range;
    }
    
    
    public K getMin()
    {
        return range.lowerEndpoint();
    }
    
    
    public K getMax()
    {
        return range.upperEndpoint();
    }
    
    
    public RangeOp getOperator()
    {
        return op;
    }
    
    
    public static <K extends Comparable<?>> Builder<K> builder()
    {
        return new Builder<>();
    }
    
    
    public static class Builder<K extends Comparable<?>>
    {
        private RangeFilter<K> instance = new RangeFilter<>();
        
        
        public Builder<K> withRange(K min, K max)
        {
            instance.range = Range.closed(min,  max);
            return this;
        }
        
        
        public Builder<K> withSingleValue(K val)
        {
            instance.range = Range.singleton(val);
            return this;
        }
        
        
        public Builder<K> withOperator(RangeOp op)
        {
            instance.op = op;
            return this;
        }
        
        
        public RangeFilter<K> build()
        {
            Asserts.checkNotNull(instance.range, "range");
            return instance;
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public boolean test(Object val)
    {
        if (val instanceof Range)
        {
            Range<K> other = (Range<K>)val;
            
            switch (op)
            {
                case CONTAINS:
                    return range.encloses(other);
                case EQUALS:
                    return range.equals(other);
                default:
                    return range.isConnected(other);
            }
        }
        else
            return range.contains((K)val);
    }
    
    
    @Override
    public String toString()
    {
        return String.format("Range %s [%s - %s]", op, range.lowerEndpoint(), range.upperEndpoint()); 
    }
    
}
