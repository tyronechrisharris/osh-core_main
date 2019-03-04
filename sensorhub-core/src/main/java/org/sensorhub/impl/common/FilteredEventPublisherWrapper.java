/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventPublisher;


public class FilteredEventPublisherWrapper implements IEventPublisher
{
    String sourceID;
    IEventPublisher wrappedPublisher;
    Predicate<Event> filter;
    

    public FilteredEventPublisherWrapper(IEventPublisher wrappedPublisher, String sourceID, final Predicate<Event> filter)
    {
        this.sourceID = sourceID;
        this.wrappedPublisher = wrappedPublisher;
        this.filter = filter;
    }


    @Override
    public void subscribe(Subscriber<? super Event> subscriber)
    {
        wrappedPublisher.subscribe(new FilteredSubscriber<Event>(subscriber, filter));
    }


    @Override
    public int getNumberOfSubscribers()
    {
        return wrappedPublisher.getNumberOfSubscribers();
    }


    @Override
    public void publish(Event e)
    {
        wrappedPublisher.publish(e);
    }


    @Override
    public void publish(Event e, BiPredicate<Subscriber<? super Event>, ? super Event> onDrop)
    {
        wrappedPublisher.publish(e, onDrop);
    }

}
