/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.event;

import java.util.ArrayList;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiPredicate;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.IEventPublisher;


/**
 * <p>
 * Class that temporarily holds list of subscribers before the publisher 
 * actually registers
 * </p>
 *
 * @author Alex Robin
 * @date Feb 21, 2019
 */
public class PlaceHolderPublisher implements IEventPublisher
{
    ArrayList<Subscriber<? super Event>> subscribers = new ArrayList<>();
    
    
    @Override
    public void subscribe(Subscriber<? super Event> subscriber)
    {
        subscribers.add(subscriber);      
    }
    
    
    public void transferTo(IEventPublisher realPublisher)
    {
        for (Subscriber<? super Event> s: subscribers)
            realPublisher.subscribe(s);
    }


    @Override
    public void publish(Event e)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void publish(Event e, BiPredicate<Subscriber<? super Event>, ? super Event> onDrop)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public int getNumberOfSubscribers()
    {
        return subscribers.size();
    }

}
