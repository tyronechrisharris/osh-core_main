/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
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
