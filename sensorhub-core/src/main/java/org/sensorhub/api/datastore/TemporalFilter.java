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
    
    
    public boolean isLatestTime()
    {
        return latestTime;
    }
    
    
    public boolean isCurrentTime()
    {
        return currentTime;
    }
    
    
    public boolean isAllTimes()
    {
        return getMin() == Instant.MIN && getMax() == Instant.MAX;
    }
    
    
    public boolean test(TimeExtent te)
    {
        if (latestTime || currentTime && te.endsNow())
            return true;
        
        return getMin().compareTo(te.end()) <= 0 &&
               getMax().compareTo(te.begin()) >= 0;
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
            this.parent = parent;
            this.instance = new TemporalFilter();
        }
                
        public abstract B done();
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class TimeFilterBuilder<
            B extends TimeFilterBuilder<B, F>,
            F extends TemporalFilter>
        extends RangeFilterBuilder<B, F, Instant>
    {
        protected TimeFilterBuilder()
        {
        }
        
        
        protected TimeFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        public B withCurrentTime()
        {
            instance.currentTime = true;
            instance.latestTime = false;
            instance.range = Range.singleton(Instant.now());
            return (B)this;
        }
        
        
        public B withLatestTime()
        {
            instance.latestTime = true;
            instance.currentTime = false;
            instance.range = Range.singleton(Instant.MAX);
            return (B)this;
        }
        
        
        public B withAllTimes()
        {
            withRange(Instant.MIN, Instant.MAX);
            return (B)this;
        }
    }
}
