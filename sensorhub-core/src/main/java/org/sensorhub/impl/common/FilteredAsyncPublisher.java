/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.common;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Predicate;


/**
 * <p>
 * Extension of {@link SubmissionPublisher} to support filtering events before
 * dispatching to clients.
 * </p>
 * <p>
 * <i>Note that this could be optimized by filtering items before they are
 * added to consumer queues, but this requires modifying SubmissionPublisher
 * internal class BufferedSubscription that is marked as final.</i>
 * </p>
 *
 * @author Alex Robin
 * @param <T> the published item type 
 * @date Feb 21, 2019
 */
public class FilteredAsyncPublisher<T> extends SubmissionPublisher<T>
{
    
    public FilteredAsyncPublisher(Executor executor, int maxBufferCapacity)
    {
        super(executor, maxBufferCapacity);
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void subscribe(final Subscriber<? super T> subscriber, final Predicate<? super T> filter)
    {
        subscribe((Subscriber<T>)new FilteredSubscriber(subscriber, filter));        
    }
}
