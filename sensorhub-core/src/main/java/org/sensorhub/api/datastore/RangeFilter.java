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
import org.vast.util.BaseBuilder;
import com.google.common.collect.Range;


/**
 * <p>
 * Filter for ranges of values.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 4, 2018
 * @param <K> Type of range bounds
 */
public class RangeFilter<K extends Comparable<?>> implements Predicate<K>
{    
    protected Range<K> range;
    protected RangeOp op = RangeOp.INTERSECTS;
    
    
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
    
    
    public boolean isSingleValue()
    {
        return range.lowerEndpoint().equals(range.upperEndpoint());
    }
    
    
    @Override
    public boolean test(K val)
    {
        return range.contains(val);
    }


    public boolean test(Range<K> other)
    {
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
    
    
    @Override
    public String toString()
    {
        return String.format("Range %s [%s - %s]", op, range.lowerEndpoint(), range.upperEndpoint()); 
    }
    
    
    /*
     * Builder
     */
    public static class Builder<T extends Comparable<T>> extends RangeFilterBuilder<Builder<T>, RangeFilter<T>, T>
    {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public Builder()
        {
            super(new RangeFilter());
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class RangeFilterBuilder<
            B extends RangeFilterBuilder<B, F, T>,
            F extends RangeFilter<T>,
            T extends Comparable<T>>
        extends BaseBuilder<F>
    {        
        protected RangeFilterBuilder()
        {
        }
        
        
        protected RangeFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        public B withRange(T min, T max)
        {
            instance.range = Range.closed(min,  max);
            return (B)this;
        }
        
        
        public B withSingleValue(T val)
        {
            instance.range = Range.singleton(val);
            return (B)this;
        }
        
        
        public B withOperator(RangeOp op)
        {
            instance.op = op;
            return (B)this;
        }
        
        
        @Override
        public F build()
        {
            Asserts.checkNotNull(instance.range, "range");
            return super.build();
        }
    }
    
}
