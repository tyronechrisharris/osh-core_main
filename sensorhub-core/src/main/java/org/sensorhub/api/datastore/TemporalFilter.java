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
import org.vast.util.TimeExtent;
import com.google.common.collect.Range;


/**
 * <p>
 * Special case of range filter for temporal values.<br/>
 * Filtering is possible by time range, time instant, or special cases of
 * 'current time' and 'latest time'.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 29, 2019
 */
public class TemporalFilter extends RangeFilter<Instant>
{
    protected boolean currentTime; // current time at the time of query evaluation
    protected boolean latestTime; // latest available time (can be in future)
    protected int currentTimeTolerance; // in millis
    
    
    public boolean isCurrentTime()
    {
        return currentTime;
    }
    
    
    public boolean isLatestTime()
    {
        return latestTime;
    }
    
    
    public boolean isAllTimes()
    {
        return getMin() == Instant.MIN && getMax() == Instant.MAX;
    }
    
    
    @Override
    public Range<Instant> getRange()
    {
        if (currentTime)
        {
            var now = Instant.now();
            range = Range.closed(
                now.minusMillis(currentTimeTolerance),
                now.plusMillis(currentTimeTolerance));
        }
        
        return range;
    }
    
    
    @Override
    public Instant getMin()
    {
        return getRange().lowerEndpoint();
    }
    
    
    @Override
    public Instant getMax()
    {
        return getRange().upperEndpoint();
    }
    
    
    @Override
    public boolean test(Instant val)
    {
        // we always return true if set to latest time since we cannot maintain
        // state of other possibly selected records here. It means that the caller
        // must enforce the latestTime filter contract via other means
        return latestTime || getRange().contains(val);
    }
    
    
    public boolean test(TimeExtent te)
    {
        if (latestTime && te.endsNow())
            return true;
        
        var range = getRange();
        return range.lowerEndpoint().compareTo(te.end()) <= 0 &&
               range.upperEndpoint().compareTo(te.begin()) >= 0;
    }
    
    
    /**
     * Computes a logical AND between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    public TemporalFilter and(TemporalFilter filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        return and(filter, new Builder()).build();
    }
    
    
    protected <F extends TemporalFilter, B extends TimeFilterBuilder<B, F>> B and(F otherFilter, B builder) throws EmptyFilterIntersection
    {
        // handle latest time special case
        if ((otherFilter.isLatestTime() && isLatestTime()) ||
            (otherFilter.isLatestTime() && isAllTimes()) ||
            (otherFilter.isAllTimes() && isLatestTime()))
            return builder.withLatestTime();
        
        // handle current time special case
        if (otherFilter.isCurrentTime() && isCurrentTime())
            return builder.withCurrentTime(Math.min(currentTimeTolerance, otherFilter.currentTimeTolerance));
        if (otherFilter.isCurrentTime() && isAllTimes())
            return builder.withCurrentTime(otherFilter.currentTimeTolerance);
        if (otherFilter.isAllTimes() && isCurrentTime())
            return builder.withCurrentTime(currentTimeTolerance);
        
        // need to call getRange() to compute current time dynamically in case
        // we need to AND it with a fixed time period
        var range = getRange();
        var otherRange = otherFilter.getRange();
        if (!range.isConnected(otherRange))
            throw new EmptyFilterIntersection();
        return builder.withRange(range.intersection(otherRange));
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends TimeFilterBuilder<Builder, TemporalFilter>
    {
        public Builder()
        {
            super(new TemporalFilter());
        }
    }
    
    
    /*
     * Nested builder for use within another builder
     */
    public static abstract class NestedBuilder<B> extends TimeFilterBuilder<NestedBuilder<B>, TemporalFilter>
    {
        B parent;
        
        public NestedBuilder(B parent)
        {
            super(new TemporalFilter());
            this.parent = parent;            
        }
                
        public abstract B done();
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class TimeFilterBuilder<
            B extends TimeFilterBuilder<B, F>,
            F extends TemporalFilter>
        extends RangeFilterBuilder<B, F, Instant>
    {
        
        protected TimeFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        /**
         * Match time stamps that are within 1 sec of the current time as
         * provided by {@link Instant#now}
         * @return This builder for chaining
         */
        public B withCurrentTime()
        {
            return withCurrentTime(1000);
        }
        
        
        /**
         * Match time stamps that are within the specified time window
         * around the current time as provided by {@link Instant#now}
         * @param toleranceMillis Half window size in milliseconds
         * @return This builder for chaining
         */
        public B withCurrentTime(int toleranceMillis)
        {
            instance.currentTime = true;
            instance.latestTime = false;
            instance.currentTimeTolerance = toleranceMillis;
            instance.getRange();
            return (B)this;
        }
        
        
        /**
         * Keep only the objects with the latest time stamp in the selected time series
         * @return This builder for chaining
         */
        public B withLatestTime()
        {
            instance.latestTime = true;
            instance.currentTime = false;
            instance.range = Range.singleton(Instant.MAX);
            return (B)this;
        }
        
        
        /**
         * Match any timestamp
         * @return This builder for chaining
         */
        public B withAllTimes()
        {
            withRange(Instant.MIN, Instant.MAX);
            return (B)this;
        }
    }
}
