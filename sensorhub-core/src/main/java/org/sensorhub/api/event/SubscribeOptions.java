/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.event;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.sensorhub.api.common.Event;
import org.vast.util.Asserts;
import org.vast.util.BaseBuilder;


/**
 * <p>
 * EventBus subscription options
 * </p>
 *
 * @author Alex Robin
 * @param <E> Type of event being subscribe to
 * @date Mar 2, 2019
 */
public class SubscribeOptions<E extends Event>
{
    Set<String> sourceIDs = new TreeSet<>();
    Set<Class<? extends E>> eventTypes = new HashSet<>();
    Predicate<? super E> filter;
    
    
    public Set<String> getSourceIDs()
    {
        return sourceIDs;
    }


    public Set<Class<? extends E>> getEventTypes()
    {
        return eventTypes;
    }


    public Predicate<? super E> getFilter()
    {
        return filter;
    }


    void validate()
    {
        Asserts.checkState(!sourceIDs.isEmpty(), "At least one sourceID must be provided");
    }
    
    
    @SuppressWarnings("unchecked")
    public static class Builder<B extends Builder<B, E>, E extends Event> extends BaseBuilder<B, SubscribeOptions<E>>
    {
        Class<E> baseEventClass;
        
        
        public Builder(Class<E> eventClass)
        {
            this(new SubscribeOptions<E>());
            baseEventClass = eventClass;
            instance.eventTypes.add(eventClass);
        }
        
        
        protected Builder(SubscribeOptions<E> instance)
        {
            super(instance);
        }
        
        
        public B withSourceID(String... sourceIDs)
        {
            return withSourceID(Arrays.asList(sourceIDs));
        }
        
        
        public B withSourceID(Collection<String> sourceIDs)
        {
            for (String id: sourceIDs)
            {
                Asserts.checkNotNull(id, "sourceID");
                instance.sourceIDs.add(id);
            }
            return (B)this;
        }
        
        
        public B withSource(IEventProducer... sources)
        {
            for (IEventProducer s: sources)
            {
                Asserts.checkNotNull(s.getEventSourceInfo().getSourceID(), "sourceID");
                instance.sourceIDs.add(s.getEventSourceInfo().getSourceID());
            }
            return (B)this;
        }
        
        
        public B withEventType(Class<? extends E>... types)
        {
            for (Class<? extends E> t: types)
            {
                Asserts.checkArgument(baseEventClass.isAssignableFrom(t), "Event type is not compatible with the base event class of this builder");
                instance.eventTypes.add(t);
            }
            return (B)this;
        }
        
        
        public B withFilter(Predicate<? super E> filter)
        {
            instance.filter = filter;
            return (B)this;
        }
        
        
        @Override
        public SubscribeOptions<E> build()
        {
            instance.validate();
            return super.build();
        }
    }
}
