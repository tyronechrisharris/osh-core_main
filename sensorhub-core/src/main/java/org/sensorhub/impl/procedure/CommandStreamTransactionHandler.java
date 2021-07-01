/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import org.sensorhub.api.command.CommandAckEvent;
import org.sensorhub.api.command.CommandData;
import org.sensorhub.api.command.CommandEvent;
import org.sensorhub.api.command.CommandStreamChangedEvent;
import org.sensorhub.api.command.CommandStreamDisabledEvent;
import org.sensorhub.api.command.CommandStreamEnabledEvent;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.CommandStreamRemovedEvent;
import org.sensorhub.api.command.ICommandAck;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventPublisher;
import org.sensorhub.impl.event.SubscriberConsumerAdapter;
import org.sensorhub.utils.DataComponentChecks;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataBlock;
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
public class CommandStreamTransactionHandler
{
    protected final ProcedureObsTransactionHandler rootHandler;
    protected final CommandStreamKey csKey;
    protected ICommandStreamInfo csInfo;
    protected String parentGroupUID;
    protected IEventPublisher dataEventPublisher;
    protected IEventPublisher ackEventPublisher;
    
    protected Map<ICommandData, Consumer<ICommandAck>> ackCallbacks;
    protected volatile Subscription ackSub;
    
    
    public CommandStreamTransactionHandler(CommandStreamKey csKey, ICommandStreamInfo csInfo, ProcedureObsTransactionHandler rootHandler)
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
            .withProcedure(csInfo.getProcedureID())
            .withRecordDescription(dataStruct)
            .withRecordEncoding(dataEncoding)
            .withValidTime(oldCsInfo.getValidTime())
            .build();
        getCommandStreamStore().replace(csKey, newCsInfo);        
        this.csInfo = newCsInfo;
        
        // send event
        var event = new CommandStreamChangedEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
        
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
        getProcedureStatusEventPublisher().publish(event);
        
        return true;
    }
    
    
    public void enable()
    {
        var event = new CommandStreamEnabledEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
        
        // subscribe to event queue
        
    }
    
    
    public void disable()
    {
        var event = new CommandStreamDisabledEvent(csInfo);
        getStatusEventPublisher().publish(event);
        getProcedureStatusEventPublisher().publish(event);
    }
    
    
    public void connectCommandReceiver(Subscriber<CommandEvent> commandSubscriber)
    {
        Asserts.checkNotNull(commandSubscriber, Subscriber.class);
        
        var dataTopic = EventUtils.getCommandStreamDataTopicID(csInfo);
        rootHandler.eventBus.newSubscription(CommandEvent.class)
            .withTopicID(dataTopic)
            .subscribe(commandSubscriber);
    }


    public void connectAckReceiver(Subscriber<CommandAckEvent> ackSubscriber)
    {
        Asserts.checkNotNull(ackSubscriber, Subscriber.class);
        
        var ackTopic = EventUtils.getCommandStreamAckTopicID(csInfo);
        rootHandler.eventBus.newSubscription(CommandAckEvent.class)
            .withTopicID(ackTopic)
            .subscribe(ackSubscriber);
    }
    
    
    public void sendCommand(DataBlock data, Consumer<ICommandAck> onAck)
    {
        var cmd = new CommandData(csKey.getInternalID(), data);
        
        // register ACK callback if provided
        if (onAck != null)
        {
            // prepare callback map and register subscriber the first time
            if (ackCallbacks == null)
            {
                ackCallbacks = new ConcurrentHashMap<>();
                connectAckReceiver(new SubscriberConsumerAdapter<CommandAckEvent>(
                    sub -> {
                        this.ackSub = sub;
                    },
                    event -> {
                        for (var ack: event.getCommandAcks())
                        {
                            var callback = ackCallbacks.remove(ack.getCommand());
                            if (callback != null)
                                callback.accept(ack);
                        }
                    }, false));
            }
            
            ackCallbacks.put(cmd, onAck);
        }
        
        getDataEventPublisher().publish(new CommandEvent(
            System.currentTimeMillis(),
            csInfo.getProcedureID().getUniqueID(),
            csInfo.getControlInputName(),
            cmd));
    }
    
    
    public void cleanup()
    {
        if (ackSub != null)
            ackSub.cancel();
    }
    
    
    public void sendAck(ICommandAck ack)
    {
        getAckEventPublisher().publish(new CommandAckEvent(
            System.currentTimeMillis(),
            csInfo.getProcedureID().getUniqueID(),
            csInfo.getControlInputName(),
            ack));
        
        // store command with its ACK in DB
        
    }
    
    
    protected synchronized IEventPublisher getDataEventPublisher()
    {
        // create event publisher if needed
        // cache it because we need it often
        if (dataEventPublisher == null)
        {
            var topic = EventUtils.getCommandStreamDataTopicID(csInfo);
            dataEventPublisher = rootHandler.eventBus.getPublisher(topic);
        }
         
        return dataEventPublisher;
    }
    
    
    protected synchronized IEventPublisher getAckEventPublisher()
    {
        // create event publisher if needed
        // cache it because we need it often
        if (ackEventPublisher == null)
        {
            var topic = EventUtils.getCommandStreamAckTopicID(csInfo);
            ackEventPublisher = rootHandler.eventBus.getPublisher(topic);
        }
         
        return ackEventPublisher;
    }
        
        
    protected synchronized IEventPublisher getStatusEventPublisher()
    {
        var topic = EventUtils.getCommandStreamStatusTopicID(csInfo);
        return rootHandler.eventBus.getPublisher(topic);
    }
    
    
    protected IEventPublisher getProcedureStatusEventPublisher()
    {
        var procUID = csInfo.getProcedureID().getUniqueID();
        var topic = EventUtils.getProcedureStatusTopicID(procUID);
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
