/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiPredicate;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.IEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Wrapper for filtered publisher also implementing IEventPublisher
 * </p>
 *
 * @author Alex Robin
 * @date Feb 21, 2019
 */
public class FilteredEventPublisher extends FilteredAsyncPublisher<Event<?>> implements IEventPublisher
{
    private static final Logger log = LoggerFactory.getLogger(FilteredEventPublisher.class);
    
    String sourceID;
    Map<IEventListener, ListenerSubscriber> listeners;
    
    
    public FilteredEventPublisher(String sourceID, Executor executor, int maxBufferCapacity)
    {
        super(executor, maxBufferCapacity);
        this.sourceID = sourceID;
        this.listeners = new ConcurrentHashMap<>();
    }


    @Override
    public void publish(Event<?> e)
    {
        this.offer(e, (sub, ev) -> {
            log.warn("{} dropped by subscriber {}", ev, sub);
            return false;
        });
    }


    @Override
    public void publish(Event<?> e, BiPredicate<Subscriber<? super Event<?>>, ? super Event<?>> onDrop)
    {
        this.offer(e, onDrop);
    }


    @Override
    public void registerListener(IEventListener listener)
    {
        subscribe(listeners.put(listener, new ListenerSubscriber(sourceID, listener)));
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        ListenerSubscriber sub = listeners.get(listener);
        if (sub != null)
            sub.cancel();
    }
}
