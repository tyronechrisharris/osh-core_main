/***************************** BEGIN COPYRIGHT BLOCK **************************

Copyright (C) 2024 Delta Air Lines, Inc. All Rights Reserved.

Notice: All information contained herein is, and remains the property of
Delta Air Lines, Inc. The intellectual and technical concepts contained herein
are proprietary to Delta Air Lines, Inc. and may be covered by U.S. and Foreign
Patents, patents in process, and are protected by trade secret or copyright law.
Dissemination, reproduction or modification of this material is strictly
forbidden unless prior written permission is obtained from Delta Air Lines, Inc.

******************************* END COPYRIGHT BLOCK ***************************/

package org.sensorhub.impl.comm;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SubmissionPublisher;
import org.sensorhub.api.comm.IMessageQueuePush;
import org.sensorhub.api.comm.MessageQueueConfig;
import org.sensorhub.api.event.Event;
import org.sensorhub.impl.event.ListenerSubscriber;


/**
 * <p>
 * Local message queue implementation based on Java Publisher.
 * </p>
 *
 * @author Alex Robin
 * @since Jan 31, 2025
 */
public class LocalMessageQueue implements IMessageQueuePush
{
    SubmissionPublisher<Message> queue;
    Map<MessageListener, ListenerSubscriber> listeners = new ConcurrentHashMap<>();
    
    
    class Message extends Event
    {
        Map<String, String> attrs;
        byte[] payload;
        
        @Override
        public String getSourceID()
        {
            return "LOCAL";
        }
    }
    
    
    public static class LocalMessageQueueConfig extends MessageQueueConfig
    {
        public LocalMessageQueueConfig()
        {
            this.moduleClass = LocalMessageQueue.class.getCanonicalName();
        }
    }
    
    
    @Override
    public void init(MessageQueueConfig config)
    {
        queue = new SubmissionPublisher<>();
    }


    @Override
    public void publish(byte[] payload)
    {
        var msg = new Message();
        msg.payload = payload;
        queue.submit(msg);
    }


    @Override
    public void publish(Map<String, String> attrs, byte[] payload)
    {
        var msg = new Message();
        msg.attrs = attrs;
        msg.payload = payload;
        queue.submit(msg);
    }


    @Override
    public void registerListener(MessageListener listener)
    {
        var sub = listeners.computeIfAbsent(listener, k -> {
            return new ListenerSubscriber(e -> {
                var attrs = ((Message)e).attrs;
                if (attrs == null)
                    attrs = Collections.emptyMap();
                var payload = ((Message)e).payload;
                listener.receive(attrs, payload);
            });
        });
            
        queue.subscribe(sub);
    }


    @Override
    public void unregisterListener(MessageListener listener)
    {
        listeners.computeIfPresent(listener, (k,v) -> {
            v.cancel();
            return null;
        });
    }


    @Override
    public void start()
    {
    }


    @Override
    public void stop()
    {
    }

}
