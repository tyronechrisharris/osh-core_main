/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.vast.ows.OWSRequest;
import org.vast.util.Asserts;


/**
 * <p>
 * Base class with common async logic for all serializers
 * </p>
 * 
 * @param <R> The service request type
 * @param <T> The serialized item type
 *
 * @author Alex Robin
 * @date Apr 16, 2020
 */
public abstract class AbstractAsyncSerializer<R, T> implements IAsyncResponseSerializer<R, T>
{
    protected SOSServlet servlet;
    protected AsyncContext asyncCtx;
    protected R request;
    //protected ServletOutputStream os;
    protected BufferedAsyncOutputStream os;
    protected Queue<T> recordQueue;
    protected Subscription subscription;
    volatile boolean done = false;
    volatile boolean beforeRecordsCalled = false;
    volatile boolean onCompleteCalled = false;
    volatile boolean writing = false;
    
    // atomic synchronization flags
    final static int READY = 0;
    final static int WRITING = 1;
    final static int MORE_TO_WRITE = 2;
    AtomicInteger cas = new AtomicInteger(READY);

    
    protected abstract void beforeRecords() throws IOException;
    protected abstract void afterRecords() throws IOException;
    protected abstract void writeRecord(T item) throws IOException;
    
    
    public void init(SOSServlet servlet, AsyncContext asyncCtx, R request) throws IOException
    {
        this.servlet = Asserts.checkNotNull(servlet, SOSServlet.class);
        this.asyncCtx = Asserts.checkNotNull(asyncCtx, AsyncContext.class);
        this.request = Asserts.checkNotNull(request, OWSRequest.class);
        this.os = new BufferedAsyncOutputStream(asyncCtx.getResponse().getOutputStream(), 16*1024);
        this.recordQueue = new ConcurrentLinkedQueue<>();
    }
    
    
    @Override
    public void onSubscribe(Subscription subscription)
    {
        this.subscription = subscription;
        os.setWriteListener(this);
        subscription.request(1);
    }
    
    
    @Override
    public void onWritePossible() throws IOException
    {
        // write everything we can while servlet output stream is ready
        // otherwise we'll be called back later
        if (os.isReady() && cas.compareAndSet(READY, WRITING))
        {
            do
            {
                if (!beforeRecordsCalled)
                {
                    beforeRecords();
                    beforeRecordsCalled = true;
                }
                
                while (!recordQueue.isEmpty() && os.isReady())
                {
                    writeRecord(recordQueue.remove());
                    subscription.request(1);
                }
                
                if (recordQueue.isEmpty() && onCompleteCalled)
                {
                    afterRecords();
                    asyncCtx.complete();
                    done = true;
                }
            }
            while (!done && cas.getAndUpdate(s -> s == MORE_TO_WRITE ? WRITING : READY) == MORE_TO_WRITE);
        }
    }
    
    
    @Override
    public void onNext(T item)
    {
        try
        {
            //System.out.println("\nonNext()");
            if (!onCompleteCalled)
            {
                if (!recordQueue.offer(item))
                    onError(new IllegalStateException("Write queue is full"));
                if (!cas.compareAndSet(WRITING, MORE_TO_WRITE))         
                    onWritePossible();
            }
        }
        catch (Throwable e)
        {
            onError(e);
        }
    }
    

    @Override
    public void onComplete()
    {        
        try
        {
            //System.out.println("onComplete()");
            if (!onCompleteCalled)
            {
                onCompleteCalled = true;
                if (!cas.compareAndSet(WRITING, MORE_TO_WRITE))
                    onWritePossible();
            }
        }
        catch (Throwable e)
        {
            onError(e);
        }
    }
    

    @Override
    public void onError(Throwable e)
    {
        servlet.handleError(
            (HttpServletRequest)asyncCtx.getRequest(),
            (HttpServletResponse)asyncCtx.getResponse(),
            null, e);
        asyncCtx.complete();
    }

}