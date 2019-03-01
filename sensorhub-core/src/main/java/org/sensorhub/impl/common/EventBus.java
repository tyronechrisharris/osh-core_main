/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.IEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;


/**
 * <p>
 * Event Bus: Main event management class in SensorHub.<br/>
 * All event producers and listeners registrations must be done through
 * this class (instead of directly with the source module) in order to
 * benefit from more advanced event dispatching implementations such as
 * distributed event messaging.<br/>
 * </p>
 *
 * @author Alex Robin
 * @since Mar 19, 2015
 */
public class EventBus
{
    private static final Logger log = LoggerFactory.getLogger(EventBus.class);
    public static final int MAX_BUFFER_CAPACITY = 1024;
    public static final String MAIN_TOPIC = "_MAIN_";
        
    private ExecutorService threadPool;
    private Map<String, IEventPublisher> publishers;
    
    
    public EventBus()
    {
        publishers = new HashMap<>();
        
        // create thread pool that will be used by all asynchronous event handlers
        threadPool = new ThreadPoolExecutor(0, 100,
                                            10L, TimeUnit.SECONDS,
                                            new SynchronousQueue<Runnable>(),
                                            new EventThreadFactory("EventBus"));
    }
    
    
    /*
     * Ensures a publisher is created for this sourceID.
     * 1. If a publisher already exists, it is returned.
     * 2. If the previously registered publisher was just a placeholder it is
     *    converted to a full fledged publisher.
     * 3. Otherwise a new publisher is created.
     */
    protected IEventPublisher ensurePublisher(final String sourceID, final Supplier<IEventPublisher> supplier)
    {
        return publishers.compute(sourceID, (id, pub) -> {            
            // if there is a placeholder, replace by actual publisher
            boolean hasPlaceHolder = pub instanceof PlaceHolderPublisher;
            if (pub == null || hasPlaceHolder)
            {
                IEventPublisher newPublisher = supplier.get();
                if (hasPlaceHolder)
                    ((PlaceHolderPublisher)pub).transferTo(newPublisher);
                return newPublisher;
            }
            else
                return pub;
        });
    }
    
    
    /**
     * Create a new publisher or return the existing one for the given source ID
     * @param sourceID ID of event source to obtain a publisher for
     * @return the publisher instance that can be used to publish events and
     * register {@link IEventListener listeners} and/or {@link Subscriber subscribers}
     */
    public synchronized IEventPublisher getPublisher(String sourceID)
    {
        Asserts.checkNotNull(sourceID, "sourceID");
        
        return ensurePublisher(sourceID,
            () -> new FilteredEventPublisher(sourceID, threadPool, MAX_BUFFER_CAPACITY) );
    }
        
    
    /**
     * Get a publisher that shares a channel with all other sources in the same group.
     * <p>All events published in the same group are delivered sequentially and their
     * order is guaranteed globally among all publishers in the group. Events are
     * dispatched in the same order they were received, even if they come from different
     * sources</p>
     * @param groupID ID of publisher group
     * @param sourceID ID of event source to obtain a publisher for
     * @return the publisher instance that can be used to publish events and
     * register {@link IEventListener listeners} and/or {@link Subscriber subscribers}
     */
    public synchronized IEventPublisher getPublisher(String groupID, String sourceID)
    {
        Asserts.checkNotNull(groupID, "groupID");
        Asserts.checkNotNull(sourceID, "sourceID");
        
        IEventPublisher groupPublisher = getPublisher(groupID);
        return ensurePublisher(sourceID,
            () -> new FilteredEventPublisherWrapper(groupPublisher, sourceID, e -> sourceID.equals(e.getSourceID())) );
    }
    
    
    /**
     * Subscribe to events generated by a single source
     * @param sourceID ID of source to subscribe to
     * @param subscriber subscriber that will receive events
     */
    public synchronized void subscribe(String sourceID, Subscriber<Event<?>> subscriber)
    {
        Asserts.checkNotNull(sourceID, "sourceID");
        Asserts.checkNotNull(subscriber, Subscriber.class);
        
        IEventPublisher publisher = publishers.computeIfAbsent(sourceID, id -> new PlaceHolderPublisher());
        publisher.subscribe(subscriber);
    }
    
    
    /**
     * Subscribe to events generated by a single source and matching the provided filter
     * @param sourceID ID of source to subscribe to
     * @param filter event filter
     * @param subscriber subscriber that will receive events
     */
    public synchronized void subscribe(String sourceID, Predicate<Event<?>> filter, Subscriber<Event<?>> subscriber)
    {
        if (filter == null)
            subscribe(sourceID, subscriber);
        else
            subscribe(sourceID, new FilteredSubscriber<Event<?>>(subscriber, filter));
    }
    
    
    /**
     * Subscribe to events generated by a single source and that are of a specified type
     * @param sourceID ID of source to subscribe to
     * @param eventClass type/class of event to receive (all events that are 
     *        or of this class or inherit from this class will be delivered) 
     * @param subscriber subscriber that will receive events
     */
    public synchronized <T extends Event<?>> void subscribe(String sourceID, Class<T> eventClass, Subscriber<T> subscriber)
    {
        subscribe(sourceID, eventClass, null, subscriber);
    }
    
    
    /**
     * Subscribe to events generated by a single source, that are of a specified type
     * and also match the provided filter
     * @param sourceID ID of source to subscribe to
     * @param eventClass type/class of event to receive
     * @param filter event filter
     * @param subscriber subscriber that will receive events
     */
    @SuppressWarnings("unchecked")
    public synchronized <T extends Event<?>> void subscribe(String sourceID, Class<T> eventClass, Predicate<T> filter, Subscriber<T> subscriber)
    {
        Asserts.checkNotNull(eventClass, Class.class);
        Asserts.checkNotNull(filter, Predicate.class);
        
        Predicate<T> combinedFilter;        
        if (filter != null)
            combinedFilter = e -> eventClass.isAssignableFrom(e.getClass()) && filter.test(e);
        else
            combinedFilter = e -> eventClass.isAssignableFrom(e.getClass());
        
        subscribe(sourceID, (Predicate<Event<?>>)combinedFilter, (Subscriber<Event<?>>)subscriber);
    }
    
    
    /**
     * Subscribe to events generated by multiple sources.<br/>
     * All events will be multiplexed and sent to the provided subscriber.
     * @param sourceIDs
     * @param subscriber
     */
    public synchronized void subscribe(Set<String> sourceIDs, Subscriber<Event<?>> subscriber)
    {
        // if sourceIDs are all multiplexed in a single publisher, use filtered subscription
        
        
        // else if sourceIDs are scattered among several publishers, use aggregate subscriber
        
    }
    
    
    public synchronized void subscribe(Set<String> sourceIDs, Subscriber<Event<?>> subscriber, Predicate<Event<?>> filter)
    {
        
    }
    
    
    /**
     * Simpler method to subscribe for event
     * @param sourceID
     * @param listener
     */
    public synchronized void registerListener(String sourceID, IEventListener listener)
    {
        subscribe(sourceID, new ListenerSubscriber(sourceID, listener));
    }


    public synchronized void unregisterListener(String sourceID, IEventListener listener)
    {
        IEventPublisher publisher = publishers.get(sourceID);
        if (publisher != null)
            publisher.unregisterListener(listener);
    }
    
    
    /**
     * Check how many subscribers are listening to events from a given source.
     * <p><i>Note: For instance, this can be used to pause the source when nothing
     * is expecting data from it</i></p>
     * @param sourceID
     * @return number of subscribers
     */
    public int getNumberOfSubscribers(String sourceID)
    {
        IEventPublisher publisher = publishers.get(sourceID);
        if (publisher != null)
            return publisher.getNumberOfSubscribers();
        else
            return 0;
    }

    
    public void shutdown()
    {
        if (threadPool != null)
            threadPool.shutdownNow();
    }
}
