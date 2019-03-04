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


/**
 * <p>
 * Wrapper for filtered publisher also implementing IEventPublisher
 * </p>
 *
 * @author Alex Robin
 * @date Feb 21, 2019
 */
public class FilteredEventPublisher extends FilteredAsyncPublisher<Event> implements IEventPublisher
{
    String sourceID;
    Map<IEventListener, ListenerSubscriber> listeners;
    Logger log;
    
    
    public FilteredEventPublisher(String sourceID, Executor executor, int maxBufferCapacity, Logger logger)
    {
        super(executor, maxBufferCapacity);
        this.sourceID = sourceID;
        this.listeners = new ConcurrentHashMap<>();
        this.log = logger;
    }


    @Override
    public void publish(Event e)
    {
        this.offer(e, (sub, ev) -> {
            log.warn("{} from {} dropped by subscriber {}", ev.getClass().getSimpleName(), sourceID, sub);
            return false;
        });
    }


    @Override
    public void publish(Event e, BiPredicate<Subscriber<? super Event>, ? super Event> onDrop)
    {
        this.offer(e, onDrop);
    }
}
