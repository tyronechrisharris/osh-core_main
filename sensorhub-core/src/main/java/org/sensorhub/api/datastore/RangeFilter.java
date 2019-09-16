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
