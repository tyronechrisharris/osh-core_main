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

import java.time.Duration;
import java.time.Instant;
import org.vast.util.Asserts;
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
    public static final Duration CURRENT_TIME_TOLERANCE_DEFAULT = Duration.ofMillis(1000);
    
    protected boolean timeRangeBeginsNow; // now = current time at the time of query evaluation
    protected boolean timeRangeEndsNow; // now = current time at the time of query evaluation
    protected boolean latestTime; // latest available time (can be in future)
    protected Duration currentTimeTolerance; // in millis
    
    
    public boolean isCurrentTime()
    {
        return timeRangeBeginsNow && timeRangeEndsNow;
    }
    
    
    public boolean isLatestTime()
    {
        return latestTime;
    }
    
    
    public boolean isAllTimes()
    {
        return getMin() == Instant.MIN && getMax() == Instant.MAX;
    }
    
    
    public boolean beginsNow()
    {
        return timeRangeBeginsNow;
    }
    
    
    public boolean endsNow()
    {
        return timeRangeEndsNow;
    }
    
    
    @Override
    public Range<Instant> getRange()
    {
        if (isCurrentTime())
        {
            var now = Instant.now();
            range = Range.closed(
                now.minus(currentTimeTolerance),
                now.plus(currentTimeTolerance));
        }
        else if (timeRangeBeginsNow)
        {
            var now = Instant.now();
            range = Range.closed(now, range.upperEndpoint());
        }
        else if (timeRangeEndsNow)
        {
            var now = Instant.now();
            range = Range.closed(range.lowerEndpoint(), now);
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
    
    
    public Duration getCurrentTimeTolerance()
    {
        return currentTimeTolerance;
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
        // always pass when latestTime is selected because it depends on other
        // elements in the time series and cannot be processed here
        if (latestTime)
            return true;
        
        var range = getRange();
        return range.lowerEndpoint().compareTo(te.end()) <= 0 &&
               range.upperEndpoint().compareTo(te.begin()) >= 0;
    }
    
    
    /**
     * Computes the intersection (logical AND) between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    public TemporalFilter intersect(TemporalFilter filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        return intersect(filter, new Builder()).build();
    }
    
    
    protected <F extends TemporalFilter, B extends TimeFilterBuilder<B, F>> B intersect(F otherFilter, B builder) throws EmptyFilterIntersection
    {
        // handle latest time special case
        if ((otherFilter.isLatestTime() && isLatestTime()) ||
            (otherFilter.isLatestTime() && isAllTimes()) ||
            (otherFilter.isAllTimes() && isLatestTime()))
            return builder.withLatestTime();
        
        // handle current time special case
        if (otherFilter.isCurrentTime() && isCurrentTime())
            return builder.withCurrentTime(currentTimeTolerance.compareTo(otherFilter.currentTimeTolerance));
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
    
    
    @Override
    public String toString()
    {
        if (isCurrentTime())
            return "current";
        else if (isLatestTime())
            return "latest";
        else
            return super.toString();
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
        
        /**
         * Builds a new filter using the provided filter as a base
         * @param base Filter used as base
         * @return The new builder
         */
        public static Builder from(TemporalFilter base)
        {
            return new Builder().copyFrom(base);
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
        
        
        protected B copyFrom(F base)
        {
            Asserts.checkNotNull(base, TemporalFilter.class);
            super.copyFrom(base);
            instance.timeRangeBeginsNow = base.timeRangeBeginsNow;
            instance.timeRangeEndsNow = base.timeRangeEndsNow;
            instance.latestTime = base.latestTime;
            instance.currentTimeTolerance = base.currentTimeTolerance;
            return (B)this;
        }
        
        
        /**
         * Match time stamps that are within 1 sec of the current time as
         * provided by {@link Instant#now}
         * @return This builder for chaining
         */
        public B withCurrentTime()
        {
            return withCurrentTime(CURRENT_TIME_TOLERANCE_DEFAULT);
        }
        
        
        /**
         * Match time stamps that are within the specified time window
         * around the current time as provided by {@link Instant#now}
         * @param toleranceMillis Half window size in milliseconds
         * @return This builder for chaining
         */
        public B withCurrentTime(int toleranceMillis)
        {
            return withCurrentTime(Duration.ofMillis(toleranceMillis));
        }
        
        
        /**
         * Match time stamps that are within the specified time window
         * around the current time as provided by {@link Instant#now}
         * @param tolerance Half window size as a duration
         * @return This builder for chaining
         */
        public B withCurrentTime(Duration tolerance)
        {
            instance.timeRangeBeginsNow = instance.timeRangeEndsNow = true;
            instance.latestTime = false;
            instance.currentTimeTolerance = tolerance;
            instance.getRange(); // precompute time range
            return (B)this;
        }
        
        
        /**
         * Match only the latest time stamp a time series
         * @return This builder for chaining
         */
        public B withLatestTime()
        {
            instance.latestTime = true;
            instance.timeRangeBeginsNow = instance.timeRangeEndsNow = false;
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
        
        
        /**
         * Match time stamps between the specified instant and the current time, both included
         * @param begin Beginning of time range
         * @return This builder for chaining
         */
        public B withRangeEndingNow(Instant begin)
        {
            instance.timeRangeEndsNow = true;
            withRange(begin, Instant.EPOCH);
            return (B)this;
        }
        
        
        /**
         * Match time stamps between the current time and the specified instant, both included
         * @param end End of time range
         * @return This builder for chaining
         */
        public B withRangeBeginningNow(Instant end)
        {
            instance.timeRangeBeginsNow = true;
            withRange(Instant.EPOCH, end);
            return (B)this;
        }
        
        
        /**
         * Build the filter using the same bounds as the provided time extent,
         * accounting for 'now' and unbounded edge cases.
         * @param te Time extent
         * @return This builder for chaining
         */
        public B fromTimeExtent(TimeExtent te)
        {
            if (te.isNow())
                return withCurrentTime(0);
            else if (te.beginsNow())
                return withRangeBeginningNow(te.end());
            else if (te.endsNow())
                return withRangeEndingNow(te.begin());
            else if (!te.hasBegin())
                return withRange(Instant.MIN, te.end());
            else if (!te.hasEnd())
                return withRange(te.begin(), Instant.MAX);
            else
                return withRange(te.begin(), te.end());
        }
    }
}
