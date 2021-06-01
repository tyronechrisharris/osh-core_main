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

import java.util.concurrent.Flow.Subscriber;
import java.util.stream.Collectors;
import org.sensorhub.api.command.CommandStreamEvent;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.obs.DataStreamEvent;
import org.sensorhub.api.obs.ObsEvent;
import org.sensorhub.api.procedure.ProcedureEvent;


/**
 * <p>
 * Helper for subscribing to procedure events, including observation events
 * and procedure change events (i.e. procedure added, updated, deleted).
 * </p><p>
 * The filter is evaluated at the time of subscription and not re-evaluated
 * afterwards, even if wildcards are used. This is to avoid unexpected data
 * to start flowing through an existing subscription. This means that if the
 * consumer wants to subscribe to events associated to procedures that joined
 * the hub after the subscription was created, the consumer must also
 * subscribe to the desired "resource added" events and update the main
 * subscription to include the new data sources.
 * </p>
 *
 * @author Alex Robin
 * @since May 26, 2021
 */
public class ProcedureSubscriptionHandler
{
    IProcedureObsDatabase db;
    IEventBus eventBus;
    
    
    public ProcedureSubscriptionHandler(IProcedureObsDatabase db, IEventBus eventBus)
    {
        this.db = db;
        this.eventBus = eventBus;
    }
    
    
    public <T extends ProcedureEvent> boolean subscribe(ProcedureFilter filter, Class<T> eventClass, Subscriber<T> subscriber)
    {
        // query all matching procedures from DB
        // and collect corresponding topics
        var topics = db.getProcedureStore().select(filter)
            .map(proc -> EventUtils.getProcedureStatusTopicID(proc.getUniqueIdentifier()))
            .collect(Collectors.toSet());
        
        if (topics.isEmpty())
            return false;
        
        // subscribe to all selected topics
        eventBus.newSubscription(eventClass)
            .withTopicIDs(topics)
            .subscribe(subscriber);
        
        return true;
    }
    
    
    public boolean subscribe(ProcedureFilter filter, Subscriber<ProcedureEvent> subscriber)
    {
        return subscribe(filter, ProcedureEvent.class, subscriber);
    }
    
    
    public boolean subscribe(DataStreamFilter filter, Subscriber<DataStreamEvent> subscriber)
    {        
        // query all matching datastreams from DB
        // and collect corresponding topics
        var topics = db.getDataStreamStore().select(filter)
            .map(dsInfo -> {
                var procUID = dsInfo.getProcedureID().getUniqueID();
                var outputName = dsInfo.getOutputName();
                return EventUtils.getDataStreamStatusTopicID(procUID, outputName);
            })
            .collect(Collectors.toSet());
        
        if (topics.isEmpty())
            return false;
        
        // subscribe to all selected topics
        eventBus.newSubscription(DataStreamEvent.class)
            .withTopicIDs(topics)
            .subscribe(subscriber);
        
        return true;
    }
    
    
    public boolean subscribe(CommandStreamFilter filter, Subscriber<CommandStreamEvent> subscriber)
    {
        // query all matching command streams from DB
        // and collect corresponding topics
        var topics = db.getCommandStreamStore().select(filter)
            .map(csInfo -> {
                var procUID = csInfo.getProcedureID().getUniqueID();
                var inputName = csInfo.getControlInputName();
                return EventUtils.getCommandStreamStatusTopicID(procUID, inputName);
            })
            .collect(Collectors.toSet());
        
        if (topics.isEmpty())
            return false;
        
        // subscribe to all selected topics
        eventBus.newSubscription(CommandStreamEvent.class)
            .withTopicIDs(topics)
            .subscribe(subscriber);
        
        return true;
    }
    
    
    public boolean subscribe(ObsFilter filter, Subscriber<ObsEvent> subscriber)
    {
        var dsFilter = filter.getDataStreamFilter();
        if (dsFilter == null)
            dsFilter = new DataStreamFilter.Builder().build();
        
        // query all matching datastreams from DB
        var topics = db.getDataStreamStore().select(dsFilter)
            .map(dsInfo -> {
                var procUID = dsInfo.getProcedureID().getUniqueID();
                var outputName = dsInfo.getOutputName();
                return EventUtils.getDataStreamDataTopicID(procUID, outputName);
            })
            .collect(Collectors.toSet());
        
        if (topics.isEmpty())
            return false;
        
        // subscribe to all selected topics
        eventBus.newSubscription(ObsEvent.class)
            .withTopicIDs(topics)
            //.withFilter(e -> filter.test(e.getObservations().iterator().next()))
            .subscribe(subscriber);
        
        return true;
    }
}
