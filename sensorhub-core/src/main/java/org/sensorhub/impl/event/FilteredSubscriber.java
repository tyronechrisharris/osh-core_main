/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.event;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Predicate;


/**
 * <p>
 * Wraps an existing subscriber and further filters items before they are
 * delivered to its {@link Subscriber#onNext(Object) onNext()} method
 * </p>
 *
 * @author Alex Robin
 * @param <T> the subscribed item type 
 * @date Feb 22, 2019
 */
public class FilteredSubscriber<T> implements Subscriber<T>
{
    Subscriber<? super T> wrappedSubscriber;
    Predicate<? super T> filter;
    Subscription subscription;
    
    
    public FilteredSubscriber(Subscriber<? super T> wrappedSubscriber, Predicate<? super T> filter)
    {
        this.wrappedSubscriber = wrappedSubscriber;
        this.filter = filter;
    }


    @Override
    public void onComplete()
    {
        wrappedSubscriber.onComplete();
    }


    @Override
    public void onError(Throwable throwable)
    {
        wrappedSubscriber.onError(throwable);
    }


    @Override
    public void onNext(T item)
    {
        if (filter.test(item))
            wrappedSubscriber.onNext(item);
        else
            subscription.request(1);
    }


    @Override
    public void onSubscribe(Subscription subscription)
    {
        this.subscription = subscription;
        wrappedSubscriber.onSubscribe(subscription);
    }
}
