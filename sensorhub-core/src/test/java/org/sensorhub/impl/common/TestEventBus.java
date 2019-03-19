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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.Test;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventPublisher;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.event.EventBus;
import org.vast.data.DataBlockInt;
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
        Predicate<Event> filter;
        ArrayList<Event> eventsReceived = new ArrayList<>(); 
        
        SubscriberInfo(String sourceId, int numRequestedEvents)
        {
            this.sourceId = sourceId;
            this.numRequestedEvents = numRequestedEvents;
            this.numExceptedEvents = numRequestedEvents;
        }
        
        SubscriberInfo(String sourceId, int numRequestedEvents, int numExceptedEvents, Predicate<Event> filter)
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
        IEventBus bus = new EventBus();
        CountDownLatch doneSignal = new CountDownLatch(subscribers.length);
        
        for (SourceInfo src: sources)
        {
            // get a publisher for this source
            if (src.parentSourceId == null)
                bus.getPublisher(src.id);
            else
                bus.getPublisher(src.parentSourceId, src.id);
        }
        
        // subscribe
        for (int i = 0; i < subscribers.length; i++)
        {
            final int subId = i;
            SubscriberInfo sub = subscribers[subId];
            
            bus.newSubscription()
                .withSourceID(sub.sourceId)
                .withFilter(sub.filter)
                .subscribe(new Subscriber<Event>() {
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
                    public void onNext(Event e)
                    {
                        long now = System.currentTimeMillis();
                        System.out.println(sub.sourceId + " -> sub" + subId + ": " + e.getTimeStamp() + " @ " + now);
                        
                        if (now - e.getTimeStamp() > 10)
                            throw new IllegalStateException("Delivery too slow!");
                        
                        sub.eventsReceived.add(e);
                        if (sub.eventsReceived.size() == sub.numExceptedEvents)
                            doneSignal.countDown();
                    }
    
                    @Override
                    public void onSubscribe(Subscription subscription)
                    {
                        System.out.println("Subscribed " + subId);
                        subscription.request(sub.numRequestedEvents);
                    }            
                });
        }
        
        int count = 0;
        boolean done = false;
        while (!done)
        {
            done = true;
            
            for (SourceInfo src: sources)
            {
                IEventPublisher pub = bus.getPublisher(src.id);
                
                DataBlockInt data = new DataBlockInt(1);
                data.setIntValue(count);
                pub.publish(new DataEvent(System.currentTimeMillis(), src.id, "test" + count, data));
                
                if (count < src.numGeneratedEvents)
                    done = false;
            }
            
            count++;
            Thread.sleep(1);
        }
        
        return doneSignal;
    }
    
    
    CountDownLatch createListenersAndSubscribe(SourceInfo[] sources, SubscriberInfo[] subscribers) throws Exception
    {
        IEventBus bus = new EventBus();
        CountDownLatch doneSignal = new CountDownLatch(subscribers.length);
        
        for (SourceInfo src: sources)
        {
            // get a publisher for this source
            if (src.parentSourceId == null)
                bus.getPublisher(src.id);
            else
                bus.getPublisher(src.parentSourceId, src.id);
        }
            
        // subscribe
        for (int i = 0; i < subscribers.length; i++)
        {
            final int subId = i;
            SubscriberInfo sub = subscribers[subId];
            
            bus.newSubscription()
                .withSourceID(sub.sourceId)
                .withFilter(sub.filter)
                .listen(e -> {
                    long now = System.currentTimeMillis();
                    System.out.println(sub.sourceId + " -> sub" + subId + ": " + e.getTimeStamp() + " @ " + now);
                    sub.eventsReceived.add(e);
                    if (sub.eventsReceived.size() == sub.numExceptedEvents)
                        doneSignal.countDown();
                });
        }
        
        int count = 0;
        boolean done = false;
        while (!done)
        {
            done = true;
            
            for (SourceInfo src: sources)
            {
                IEventPublisher pub = bus.getPublisher(src.id);
                
                DataBlockInt data = new DataBlockInt(1);
                data.setIntValue(count);
                pub.publish(new DataEvent(System.currentTimeMillis(), src.id, "test" + count, data));
                
                if (count < src.numGeneratedEvents)
                    done = false;
            }
            
            count++;
            Thread.sleep(1);
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
            new SubscriberInfo(SOURCES[0], 10, 1, e -> ((DataEvent)e).getProcedureID().equals("test2"))
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
            new SubscriberInfo(SOURCES[0], 15, 1, e -> ((DataEvent)e).getProcedureID().equals("test2")),
            new SubscriberInfo(SOURCES[0], 15),
            new SubscriberInfo(SOURCES[0], 5, 3, e -> ((DataEvent)e).getProcedureID().matches("test[0-2]"))
        };
        
        CountDownLatch doneSignal = createPublishersAndSubscribe(sources, subscribers);
        assertTrue("Timeout before enough events received", doneSignal.await(2, TimeUnit.SECONDS));        
        for (SubscriberInfo sub: subscribers)
            assertEquals(sub.numExceptedEvents, sub.eventsReceived.size());
    }
    
    
    @Test
    public void testSeveralPublisherSeveralSubscribers() throws Exception
    {
        SourceInfo[] sources = {
            new SourceInfo(SOURCES[0], 20),
            new SourceInfo(SOURCES[1], 50),
            new SourceInfo(SOURCES[4], 210),
            new SourceInfo(SOURCES[3], 160)
        };
        
        SubscriberInfo[] subscribers = {
            new SubscriberInfo(SOURCES[0], 10),
            new SubscriberInfo(SOURCES[3], 15, 15, e -> ((DataEvent)e).getRecords()[0].getIntValue() % 3 == 0),
            new SubscriberInfo(SOURCES[4], 150),
            new SubscriberInfo(SOURCES[0], 5, 3, e -> ((DataEvent)e).getProcedureID().matches("test[0-2]"))
        };
        
        CountDownLatch doneSignal = createPublishersAndSubscribe(sources, subscribers);
        assertTrue("Timeout before enough events received", doneSignal.await(2, TimeUnit.SECONDS));        
        for (SubscriberInfo sub: subscribers)
            assertEquals(sub.numExceptedEvents, sub.eventsReceived.size());
    }
    
    
    @Test
    public void testFilterByEventClass() throws InterruptedException
    {
        EventBus bus = new EventBus();
        IEventPublisher pub = bus.getPublisher(SOURCES[0]);
        
        CountDownLatch doneSignal = new CountDownLatch(5);
        AtomicInteger count = new AtomicInteger();
        
        bus.newSubscription(DataEvent.class)
            .withSourceID(SOURCES[0])
            .subscribe(new Subscriber<DataEvent>() {
                @Override
                public void onNext(DataEvent item)
                {
                    System.out.println(item.getClass());
                    count.incrementAndGet();
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
                pub.publish(new FoiEvent(System.currentTimeMillis(), "procedure", "source", "foi", 1.0));
        }
        
        assertTrue("Not enough events received", doneSignal.await(2, TimeUnit.SECONDS));
    }
    
    
    protected long publishAndCheckReceived(EventBus bus, int numGeneratedEvents, String... sources) throws InterruptedException
    {
        System.out.println("Publishing " + numGeneratedEvents + " events from " + sources.length + " source(s)");
        
        int numExpectedEvents = numGeneratedEvents*sources.length;
        CountDownLatch doneSignal = new CountDownLatch(numExpectedEvents);
        AtomicInteger count = new AtomicInteger();
        
        bus.newSubscription(DataEvent.class)
            .withSourceID(sources)
            .listen(e -> {
                count.incrementAndGet();
                doneSignal.countDown();
            });
        
        IEventPublisher[] pubs = new IEventPublisher[sources.length];
        for (int j = 0; j < sources.length; j++)
            pubs[j] = bus.getPublisher(sources[j]);
    
        long sleepTime = 5000;
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < numGeneratedEvents; i++)
        {
            for (int j = 0; j < sources.length; j++)
                pubs[j].publish(new DataEvent(System.currentTimeMillis(), sources[j], "channel", (DataBlock[])null));
            long sleepEnd = System.nanoTime() + sleepTime;
            while (System.nanoTime() < sleepEnd);
        }
        
        assertTrue("Timeout before enough events received", doneSignal.await(2, TimeUnit.SECONDS));
        assertEquals("Incorrect number of events received", numExpectedEvents, count.get());
        return System.currentTimeMillis() - t0 - (sleepTime*numGeneratedEvents/1000000);
    }
    
    
    @Test
    public void testListenOneType() throws InterruptedException
    {
        EventBus bus = new EventBus();
        IEventPublisher[] pubs = new IEventPublisher[SOURCES.length];
        for (int i = 0; i < SOURCES.length; i++)
            pubs[i] = bus.getPublisher(SOURCES[i]);
        
        publishAndCheckReceived(bus, 10, SOURCES[1]);
        publishAndCheckReceived(bus, 100, SOURCES[0], SOURCES[3]);
        publishAndCheckReceived(bus, 33, SOURCES[4], SOURCES[1]);
        publishAndCheckReceived(bus, 25, SOURCES[4], SOURCES[1], SOURCES[2]);
    }
    
    
    /*@Test
    public void testThroughput() throws InterruptedException
    {
        EventBus bus = new EventBus();
        int numSources, numEvents;
        long dt;
        
        numSources = 1;
        IEventPublisher[] pubs = new IEventPublisher[numSources];
        for (int i = 0; i < numSources; i++)
            pubs[i] = bus.getPublisher(SOURCES[i]);
        
        dt = publishAndCheckReceived(bus, numEvents = 100, SOURCES[0]);
        System.out.println(numEvents*1000/dt + " events/s");
        
        dt = publishAndCheckReceived(bus, numEvents = 1000000, SOURCES[0]);
        System.out.println(numEvents*1000/dt + " events/s");
    }*/

}
