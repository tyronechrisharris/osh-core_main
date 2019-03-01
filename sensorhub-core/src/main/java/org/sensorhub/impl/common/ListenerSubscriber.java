/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.sensorhub.api.common.Event;
import org.sensorhub.api.common.IEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * Adapts an {@link IEventListener} to the {@link Subscriber} interface
 * </p>
 *
 * @author Alex Robin
 * @date Feb 21, 2019
 */
public class ListenerSubscriber implements Subscriber<Event<?>>
{
    private static final Logger log = LoggerFactory.getLogger(ListenerSubscriber.class);
    
    String sourceID;
    IEventListener listener;
    Subscription subscription;
    boolean canceled;
    
    
    public ListenerSubscriber(String sourceID, IEventListener listener)
    {
        this.sourceID = sourceID;
        this.listener = listener;
    }
    
    
    @Override
    public void onComplete()
    {
    }
    

    @Override
    public void onError(Throwable ex)
    {
        log.error("Error while registering listener for {}", sourceID, ex);
    }
    

    @Override
    public void onNext(Event<?> e)
    {
        if (!canceled)
            listener.handleEvent(e);
    }
    

    @Override
    public void onSubscribe(Subscription subscription)
    {
        this.subscription = subscription;
        subscription.request(Long.MAX_VALUE);
    }
    
    
    public void cancel()
    {
        canceled = true;
        subscription.cancel();
    }
}
