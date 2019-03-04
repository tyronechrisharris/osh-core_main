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
import org.vast.util.Asserts;


/**
 * <p>
 * Base for wrapping existing subscribers
 * </p>
 *
 * @author Alex Robin
 * @param <T> the subscribed item type
 * @date Feb 21, 2019
 */
public class DelegateSubscriber<T> implements Subscriber<T>
{
    Subscriber<? super T> delegate;
    
    
    public DelegateSubscriber(Subscriber<? super T> delegate)
    {
        Asserts.checkNotNull(delegate, Subscriber.class);
        this.delegate = delegate;
    }
    
    
    @Override
    public void onComplete()
    {
        delegate.onComplete();
    }
    

    @Override
    public void onError(Throwable ex)
    {
        delegate.onError(ex);
    }
    

    @Override
    public void onNext(T e)
    {
        delegate.onNext(e);
    }
    

    @Override
    public void onSubscribe(Subscription sub)
    {
        delegate.onSubscribe(sub);
    }
}
