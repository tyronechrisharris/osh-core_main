/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.junit.Test;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventPublisher;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.FoiEvent;
import net.opengis.swe.v20.DataBlock;


public class TestEventBus
{
    final static String[] SOURCES = {
        "urn:test:source0",
        "urn:test:source1",
        "urn:test:source2",
        "urn:test:source3",
        "urn:test:source4"
    };
    
    
    static class SourceInfo
    {
        String id;
        String parentSourceId;
        int numGeneratedEvents = 10;
        
        SourceInfo(String id, int numGeneratedEvents)
        {
            this.id = id;
            this.numGeneratedEvents = numGeneratedEvents;
        }
        
        SourceInfo(String parentSourceId, String id, int numGeneratedEvents)
        {
            this(id, numGeneratedEvents);
            this.parentSourceId = parentSourceId;
        }
    }
    
    
    static class SubscriberInfo
    {
        String sourceId;
        int numRequestedEvents;
        int numExceptedEvents;
        Predicate<Event<?>> filter;
        ArrayList<Event<?>> eventsReceived = new ArrayList<>(); 
        
        SubscriberInfo(String sourceId, int numRequestedEvents)
        {
            this.sourceId = sourceId;
            this.numRequestedEvents = numRequestedEvents;
            this.numExceptedEvents = numRequestedEvents;
        }
        
        SubscriberInfo(String sourceId, int numRequestedEvents, int numExceptedEvents, Predicate<Event<?>> filter)
        {
            this.sourceId = sourceId;
            this.numRequestedEvents = numRequestedEvents;
            this.numExceptedEvents = numExceptedEvents;
            this.filter = filter;
        }
        
        boolean done()
        {
            return eventsReceived.size() >= numExceptedEvents;
        }
    }
    
    
    CountDownLatch createPublishersAndSubscribe(SourceInfo[] sources, SubscriberInfo[] subscribers) throws Exception
    {
        EventBus bus = new EventBus();
        CountDownLatch doneSignal = new CountDownLatch(subscribers.length);
        
        for (SourceInfo src: sources)
        {
            // get a publisher for this source
            IEventPublisher pub;
            if (src.parentSourceId == null)
                pub = bus.getPublisher(src.id);
            else
                pub = bus.getPublisher(src.parentSourceId, src.id);
            
            // subscribe
            for (SubscriberInfo sub: subscribers)
            {
                if (!sub.sourceId.equals(src.id))
                    continue;
                
                bus.subscribe(src.id, sub.filter, new Subscriber<Event<?>>() {
                    @Override
                    public void onComplete()
                    {
                        System.out.println("Publisher done");
                    }
    
                    @Override
                    public void onError(Throwable throwable)
                    {
                        System.err.println("Publisher error: " + throwable.getMessage());
                    }
    
                    @Override
                    public void onNext(Event<?> e)
                    {
                        synchronized(subscribers) 
                        {
                            long now = System.currentTimeMillis();
                            System.out.println("Received event " + e.getTimeStamp() + " @ " + now);
                            
                            if (now - e.getTimeStamp() > 10)
                                throw new IllegalStateException("Delivery too slow!");
                            
                            sub.eventsReceived.add(e);
                            if (sub.eventsReceived.size() == sub.numExceptedEvents)
                                doneSignal.countDown();
                        }
                    }
    
                    @Override
                    public void onSubscribe(Subscription subscription)
                    {
                        System.out.println("Subscribed");
                        subscription.request(sub.numRequestedEvents);
                    }            
                });
            }
            
            for (int i=0; i<src.numGeneratedEvents; i++)
            {
                pub.publish(new DataEvent(System.currentTimeMillis(), SOURCES[0], "test" + i, (DataBlock[])null));
                Thread.sleep(10);
            }
        }
        
        return doneSignal;
    }
    
    
    @Test
    public void testOnePublisherOneSubscriber() throws Exception
    {
        SourceInfo[] sources = {
            new SourceInfo(SOURCES[0], 10)
        };
        
        SubscriberInfo[] subscribers = {
            new SubscriberInfo(SOURCES[0], 10)
        };
        
        CountDownLatch doneSignal = createPublishersAndSubscribe(sources, subscribers);
        assertTrue("Timeout before enough events received", doneSignal.await(2, TimeUnit.SECONDS));        
        for (SubscriberInfo sub: subscribers)
            assertEquals(sub.numExceptedEvents, sub.eventsReceived.size());
    }
    
    
    @Test
    public void testOnePublisherOneSubscriberWithFilter() throws Exception
    {
        SourceInfo[] sources = {
            new SourceInfo(SOURCES[0], 10)
        };
        
        SubscriberInfo[] subscribers = {
            new SubscriberInfo(SOURCES[0], 10, 1, e -> ((DataEvent)e).getChannelID().equals("test2"))
        };
        
        CountDownLatch doneSignal = createPublishersAndSubscribe(sources, subscribers);
        assertTrue("Timeout before enough events received", doneSignal.await(2, TimeUnit.SECONDS));        
        for (SubscriberInfo sub: subscribers)
            assertEquals(sub.numExceptedEvents, sub.eventsReceived.size());
    }
    
    
    @Test
    public void testOnePublisherSeveralSubscribers() throws Exception
    {
        SourceInfo[] sources = {
            new SourceInfo(SOURCES[0], 20)
        };
        
        SubscriberInfo[] subscribers = {
            new SubscriberInfo(SOURCES[0], 10),
            new SubscriberInfo(SOURCES[0], 15, 1, e -> ((DataEvent)e).getChannelID().equals("test2")),
            new SubscriberInfo(SOURCES[0], 15),
            new SubscriberInfo(SOURCES[0], 5, 3, e -> ((DataEvent)e).getChannelID().matches("test[0-2]"))
        };
        
        CountDownLatch doneSignal = createPublishersAndSubscribe(sources, subscribers);
        assertTrue("Timeout before enough events received", doneSignal.await(2, TimeUnit.SECONDS));        
        for (SubscriberInfo sub: subscribers)
            assertEquals(sub.numExceptedEvents, sub.eventsReceived.size());
    }
    
    
    @Test
    public void testFilterByEventClass() throws Exception
    {
        EventBus bus = new EventBus();
        IEventPublisher pub = bus.getPublisher(SOURCES[0]);
        CountDownLatch doneSignal = new CountDownLatch(5);
        
        bus.subscribe(SOURCES[0], DataEvent.class, new Subscriber<DataEvent>() {

            @Override
            public void onNext(DataEvent item)
            {
                System.out.println(item.getClass());
                doneSignal.countDown();
            }

            @Override
            public void onSubscribe(Subscription subscription)
            {
                subscription.request(Long.MAX_VALUE);                
            }

            @Override
            public void onComplete()
            {                
            }

            @Override
            public void onError(Throwable throwable)
            {                
            }            
        });
        
        for (int i =0; i < 10; i++)
        {
            if (i % 2 == 0)
                pub.publish(new DataEvent(System.currentTimeMillis(), "source", "channel", (DataBlock[])null));
            else
                pub.publish(new FoiEvent(System.currentTimeMillis(), "source", "foi", 1.0));
        }
        
        assertTrue("Not enough events received", doneSignal.await(2, TimeUnit.SECONDS));
    }

}
