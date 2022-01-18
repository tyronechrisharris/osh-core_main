/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.system;

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.sensorhub.api.command.CommandStatusEvent;
import org.sensorhub.api.command.CommandData;
import org.sensorhub.api.command.CommandEvent;
import org.sensorhub.api.command.CommandStatus;
import org.sensorhub.api.command.CommandStreamChangedEvent;
import org.sensorhub.api.command.CommandStreamDisabledEvent;
import org.sensorhub.api.command.CommandStreamEnabledEvent;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.CommandStreamRemovedEvent;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.impl.event.DelegateSubscriber;
import org.sensorhub.impl.event.DelegatingSubscriberAdapter;
import org.sensorhub.utils.DataComponentChecks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Helper class for creating/updating/deleting command streams and persisting 
 * command messages in the associated datastore, as well as publishing the
 * corresponding events.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 21, 2021
 */
public class CommandStreamTransactionHandler implements IEventListener
{
    static Logger log = LoggerFactory.getLogger(CommandStreamTransactionHandler.class);
    protected final SystemDatabaseTransactionHandler rootHandler;
    protected final CommandStreamKey csKey;
    protected ICommandStreamInfo csInfo;
    protected String parentGroupUID;
    protected IEventPublisher dataEventPublisher;
    protected IEventPublisher cmdStatusEventPublisher;
    
    
    public CommandStreamTransactionHandler(CommandStreamKey csKey, ICommandStreamInfo csInfo, SystemDatabaseTransactionHandler rootHandler)
    {
        this.csKey = csKey;
        this.csInfo = csInfo;
        this.rootHandler = rootHandler;
    }
    
    
    public boolean update(DataComponent dataStruct, DataEncoding dataEncoding)
    {
        var oldCsInfo = getCommandStreamStore().get(csKey);
        if (oldCsInfo == null)
            return false;
        
        // check if command stream already has commands
        var hasCmd = oldCsInfo.getIssueTimeRange() != null;
        if (hasCmd &&
            (!DataComponentChecks.checkStructCompatible(oldCsInfo.getRecordStructure(), dataStruct) ||
             !DataComponentChecks.checkEncodingEquals(oldCsInfo.getRecordEncoding(), dataEncoding)))
            throw new IllegalArgumentException("Cannot update the record structure or encoding of a command interface if it already has received commands");
        
        // update datastream info
        var newCsInfo = new CommandStreamInfo.Builder()
            .withName(oldCsInfo.getName())
            .withDescription(oldCsInfo.getDescription())
            .withSystem(csInfo.getSystemID())
            .withRecordDescription(dataStruct)
            .withRecordEncoding(dataEncoding)
            .withValidTime(oldCsInfo.getValidTime())
            .build();
        getCommandStreamStore().replace(csKey, newCsInfo);
        this.csInfo = newCsInfo;
        
        // send event
        var event = new CommandStreamChangedEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getSystemStatusEventPublisher().publish(event);
        
        return true;
    }
    
    
    public boolean delete()
    {
        var oldCsKey = getCommandStreamStore().remove(csKey);
        if (oldCsKey == null)
            return false;
        
        // if command stream was currently valid, disable it first
        if (csInfo.getValidTime().endsNow())
            disable();
        
        // send event
        var event = new CommandStreamRemovedEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getSystemStatusEventPublisher().publish(event);
        
        return true;
    }
    
    
    public void enable()
    {
        var event = new CommandStreamEnabledEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getSystemStatusEventPublisher().publish(event);
    }
    
    
    public void disable()
    {
        var event = new CommandStreamDisabledEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getSystemStatusEventPublisher().publish(event);
    }
    
    
    public void connectCommandReceiver(Subscriber<CommandEvent> subscriber)
    {
        Asserts.checkNotNull(subscriber, Subscriber.class);
        
        var dataTopic = EventUtils.getCommandDataTopicID(csInfo);
        rootHandler.eventBus.newSubscription(CommandEvent.class)
            .withTopicID(dataTopic)
            .subscribe(new DelegateSubscriber<CommandEvent>(subscriber) {
                @Override
                public void onNext(CommandEvent e)
                {
                    log.debug("Received command {}: {}", e.getCorrelationID(), e.getCommand());
                    
                    // need to use internal stream ID
                    var cmd = CommandData.Builder.from(e.getCommand())
                        .withCommandStream(csKey.getInternalID())
                        .build();
                    
                    // add command to DB
                    var cmdKey = rootHandler.db.getCommandStore().add(cmd);
                    
                    // forward to command receiver 
                    e.getCommand().assignID(cmdKey);
                    super.onNext(e);
                }
            });
    }


    public void connectStatusReceiver(Subscriber<CommandStatusEvent> subscriber)
    {
        Asserts.checkNotNull(subscriber, Subscriber.class);
        
        var statusTopic = EventUtils.getCommandStatusTopicID(csInfo);
        rootHandler.eventBus.newSubscription(CommandStatusEvent.class)
            .withTopicID(statusTopic)
            .subscribe(subscriber);
    }


    /**
     * Connect status receiver for a specific command
     * @param correlationID
     * @param subscriber
     */
    public void connectStatusReceiver(long correlationID, Subscriber<ICommandStatus> subscriber)
    {
        Asserts.checkNotNull(subscriber, Subscriber.class);
        
        // create filtered subscriber specific for this command
        // subscription will be automatically canceled at the end
        var cmdSubscriber = new DelegatingSubscriberAdapter<CommandStatusEvent, ICommandStatus>(subscriber) {
            Subscription subscription;
            BigInteger cmdID = null;

            @Override
            public void onSubscribe(Subscription subscription)
            {
                this.subscription = subscription;
                subscriber.onSubscribe(subscription);
            }
            
            @Override
            public void onNext(CommandStatusEvent event)
            {
                boolean isMyCommand = false;
                
                // initially use correlation ID to get command ID
                // then compare command IDs because correlation ID may not be set afterwards
                if (event.getStatus().getCommandID().equals(cmdID))
                {
                    isMyCommand = true;
                }
                else if (event.getCorrelationID() == correlationID)
                {
                    isMyCommand = true;
                    cmdID = event.getStatus().getCommandID();
                }
                
                if (isMyCommand)
                {
                    log.debug("Received status {}: {}", event.getCorrelationID(), event.getStatus());
                    subscriber.onNext(event.getStatus());
                    
                    // cancel subscription if this status is final
                    if (event.getStatus().getStatusCode().isFinal())
                    {
                        subscriber.onComplete();
                        subscription.cancel();
                    }
                }
                else
                    subscription.request(1);
            }
        };
        
        // register subscriber specific for this command
        var statusTopic = EventUtils.getCommandStatusTopicID(csInfo);
        rootHandler.eventBus.newSubscription(CommandStatusEvent.class)
            .withTopicID(statusTopic)
            .subscribe(cmdSubscriber);
    }
    
    
    /**
     * Submit a command and receive status reports asynchronously
     * @param correlationID Correlation ID to attach to the command
     * @param cmd The command to submit
     * @param subscriber Subscriber that will be notified every time a new
     * status report is available for the command.
     */
    public void submitCommand(long correlationID, ICommandData cmd, Subscriber<ICommandStatus> subscriber)
    {
        // check command stream is correct
        Asserts.checkArgument(cmd.getCommandStreamID() == csKey.getInternalID(), "Invalid command stream ID");
        
        // TODO validate command
        
        // register status callback if provided
        if (subscriber != null)
            connectStatusReceiver(correlationID, subscriber);
        
        // send command to bus
        getCommandEventPublisher().publish(new CommandEvent(
            System.currentTimeMillis(),
            csInfo.getSystemID().getUniqueID(),
            csInfo.getControlInputName(),
            cmd, correlationID));
    }
    
    
    /**
     * Submit a command w/o registering for status updates
     * @param correlationID Correlation ID to attach to the command
     * @param cmd The command to submit
     */
    public void submitCommandNoStatus(long correlationID, ICommandData cmd)
    {
        submitCommand(correlationID, cmd, null);
    }
    
    
    /**
     * Submit a command and receive only the first status report via a future
     * @param correlationID Correlation ID to attach to the command
     * @param cmd The command to submit
     * @return A future that will complete when the initial status report is received
     * (this initial status report contains the ID assigned to the command)
     */
    public CompletableFuture<ICommandStatus> submitCommand(long correlationID, ICommandData cmd)
    {
        // adapt future with full subscriber
        var future = new CompletableFuture<ICommandStatus>();
        var subscriber = new Subscriber<ICommandStatus>() {
            Subscription subscription;
            
            @Override
            public void onSubscribe(Subscription subscription)
            {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(ICommandStatus item)
            {
                subscription.cancel();
                future.complete(item);
            }

            @Override
            public void onError(Throwable e)
            {
                future.completeExceptionally(e);
            }

            @Override
            public void onComplete()
            {
            }
        };
        
        submitCommand(correlationID, cmd, subscriber);
        return future;
    }
    
    
    public BigInteger sendStatus(long correlationID, ICommandStatus status)
    {
        log.debug("Sending status {}: {}", correlationID, status);
        
        // convert command ID to public ID
        var publicStatus = CommandStatus.Builder.from(status)
            .withCommand(rootHandler.toPublicId(status.getCommandID()))
            .build();
        
        // forward to event bus
        getCommandStatusEventPublisher().publish(new CommandStatusEvent(
            System.currentTimeMillis(),
            csInfo.getSystemID().getUniqueID(),
            csInfo.getControlInputName(),
            correlationID,
            publicStatus));
        
        // store command status in DB
        return rootHandler.db.getCommandStatusStore().add(status);
    }


    @Override
    public void handleEvent(Event e)
    {
        if (e instanceof CommandStatusEvent)
        {
            var status = ((CommandStatusEvent) e).getStatus();
            sendStatus(-1, status); // no correlation ID on subsequent events
        }
    }
    
    
    protected synchronized IEventPublisher getCommandEventPublisher()
    {
        // create event publisher if needed
        // cache it because we need it often
        if (dataEventPublisher == null)
        {
            var topic = EventUtils.getCommandDataTopicID(csInfo);
            dataEventPublisher = rootHandler.eventBus.getPublisher(topic);
        }
         
        return dataEventPublisher;
    }
    
    
    protected synchronized IEventPublisher getCommandStatusEventPublisher()
    {
        // create event publisher if needed
        // cache it because we need it often
        if (cmdStatusEventPublisher == null)
        {
            var topic = EventUtils.getCommandStatusTopicID(csInfo);
            cmdStatusEventPublisher = rootHandler.eventBus.getPublisher(topic);
        }
         
        return cmdStatusEventPublisher;
    }
        
        
    protected synchronized IEventPublisher getStatusEventPublisher()
    {
        var topic = EventUtils.getCommandStreamStatusTopicID(csInfo);
        return rootHandler.eventBus.getPublisher(topic);
    }
    
    
    protected IEventPublisher getSystemStatusEventPublisher()
    {
        var sysUID = csInfo.getSystemID().getUniqueID();
        var topic = EventUtils.getSystemStatusTopicID(sysUID);
        return rootHandler.eventBus.getPublisher(topic);
    }
    
    
    protected ICommandStreamStore getCommandStreamStore()
    {
        return rootHandler.db.getCommandStreamStore();
    }
    
    
    public CommandStreamKey getCommandStreamKey()
    {
        return csKey;
    }
    
    
    public ICommandStreamInfo getCommandStreamInfo()
    {
        return csInfo;
    }
}
