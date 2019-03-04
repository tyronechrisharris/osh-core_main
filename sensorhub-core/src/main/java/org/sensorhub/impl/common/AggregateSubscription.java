/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import org.sensorhub.api.common.Event;


/**
 * <p>
 * Subscribes to multiple channels and delivers multiplexed events.
 * This class tries to order events by time if they are received within a
 * small enough time window.
 * </p>
 *
 * @author Alex Robin
 * @date Feb 21, 2019
 */
public class AggregateSubscription implements Subscription, Subscriber<Event>
{
    Collection<Subscription> subscriptions;
    Subscriber<Event> target;
    PriorityQueue<Event> queue;
    AtomicLong numRequestedEvents = new AtomicLong();
    
    
    public AggregateSubscription()
    {
        this.queue = new PriorityQueue<>(10, (e1, e2) ->
            Long.compare(e1.getTimeStamp(), e2.getTimeStamp()));
    }
    
    
    @Override
    public void onComplete()
    {
       
    }
    

    @Override
    public void onError(Throwable throwable)
    {
        target.onError(throwable);
    }
    

    @Override
    public void onNext(Event e)
    {
        // add to sorted
        queue.add(e);
        
        target.onNext(e);
        numRequestedEvents.decrementAndGet();
    }
    

    @Override
    public void onSubscribe(Subscription subscription)
    {
        // TODO Auto-generated method stub
        
    }
    

    @Override
    public void cancel()
    {
        for (Subscription sub: subscriptions)
            sub.cancel();        
    }
    

    @Override
    public void request(long n)
    {
        for (Subscription sub: subscriptions)
            sub.request(n);
        numRequestedEvents.addAndGet(n);
    }

}
