/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Stream;
import org.sensorhub.api.command.CommandData;
import org.sensorhub.api.command.CommandEvent;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.system.CommandStreamTransactionHandler;
import org.sensorhub.impl.system.SystemUtils;
import org.vast.data.AbstractDataBlock;
import org.vast.data.DataBlockMixed;
import org.vast.data.DataBlockString;
import org.vast.data.SWEFactory;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataChoice;
import net.opengis.swe.v20.DataComponent;


public class SystemTaskingConnector implements ISPSConnector
{
    static final String WRAPPER_CHOICE_NAME = "root";
    final SPSServlet servlet;
    final SystemTaskingConnectorConfig config;
    final IObsSystemDatabase db;
    final IEventBus eventBus;
    final String sysUID;
    Map<String, CommandStreamTransactionHandler> txnHandlers = new HashMap<>();
    List<String> possibleCommandNames;
    String singleCommandName;
    Random random = new Random();
    
    
    public SystemTaskingConnector(final SPSService service, final SystemTaskingConnectorConfig config)
    {
        this.servlet = Asserts.checkNotNull(service.getServlet(), SPSServlet.class);
        this.config = Asserts.checkNotNull(config, SPSConnectorConfig.class);
        this.sysUID = Asserts.checkNotNullOrEmpty(config.systemUID, "sysUID");
        this.db = Asserts.checkNotNull(service.getReadDatabase(), IObsSystemDatabase.class);
        this.eventBus = service.getParentHub().getEventBus();
    }
    
    
    @Override
    public Stream<AbstractProcess> getProcedureDescriptions(TimeExtent timeRange)
    {
        // build filter
        var procFilter = new SystemFilter.Builder()
            .withUniqueIDs(sysUID);
        if (timeRange != null)
            procFilter.withValidTimeDuring(timeRange);
        else
            procFilter.withCurrentVersion();
        
        return db.getSystemDescStore().selectEntries(procFilter.build())
            .map(entry -> {
                var internalID = entry.getKey().getInternalID();
                var smlProc = entry.getValue().getFullDescription();
                
                // add outputs and control inputs
                return SystemUtils.addIOsFromDataStore(internalID, smlProc, db);
            });
    }
    
    
    @Override
    public DataComponent getTaskingParams()
    {
        // process all procedure datastreams
        var csFilter = new CommandStreamFilter.Builder()
            .withSystems().withUniqueIDs(sysUID).done()
            .build();
        
        // always wrap with the choice so we have the control input name
        var commandChoice = new SWEFactory().newDataChoice();
        commandChoice.setName(WRAPPER_CHOICE_NAME);
        
        db.getCommandStreamStore().select(csFilter)
           .forEach(csInfo -> {
               commandChoice.addItem(csInfo.getControlInputName(), csInfo.getRecordStructure());
           });
        
        if (commandChoice.getNumItems() == 1)
            return commandChoice.getComponent(0);
        else
            return commandChoice;
    }
    
    
    @Override
    public void startDirectTasking(DataComponent taskingParams)
    {
        getCommandNames(taskingParams);
    }
    
    
    protected void getCommandNames(DataComponent taskingParams)
    {
        if (taskingParams instanceof DataChoice && WRAPPER_CHOICE_NAME.equals(taskingParams.getName()))
        {
            possibleCommandNames = new ArrayList<>();
            for (var item: ((DataChoice)taskingParams).getItemList())
                possibleCommandNames.add(item.getName());
        }
        else
            singleCommandName = taskingParams.getName();
    }
    
    
    @Override
    public CompletableFuture<ICommandStatus> sendCommand(DataBlock data, boolean waitForStatus)
    {
        var commandName = singleCommandName;
        
        // if wrapped with a choice, extract actual command name and data
        if (commandName == null)
        {
            int selectedIndex = data.getIntValue(0);
            commandName = possibleCommandNames.get(selectedIndex);
            data = ((DataBlockMixed)data).getUnderlyingObject()[1];
        }
        
        // reuse of create new transaction handler
        var txnHandler = txnHandlers.computeIfAbsent(commandName, key -> {
            return servlet.getTransactionHandler().getCommandStreamHandler(sysUID, key);
        });
        
        // create the command
        var cmd = new CommandData.Builder()
            .withSender(servlet.getCurrentUser())
            .withCommandStream(txnHandler.getCommandStreamKey().getInternalID())
            .withParams(data)
            .build();
        
        CompletableFuture<ICommandStatus> future;
        if (waitForStatus)
        {
            future = txnHandler.submitCommand(random.nextLong(), cmd);
        }
        else
        {
            txnHandler.submitCommandNoStatus(random.nextLong(), cmd);
            future = CompletableFuture.completedFuture(null);
        }
        
        return future;
    }
    
    
    @Override
    public void subscribeToCommands(DataComponent taskingParams, Subscriber<DataBlock> subscriber)
    {
        getCommandNames(taskingParams);
        boolean multiCommandStreams = possibleCommandNames != null;
        
        // collect topics for all control inputs
        var topicList = new ArrayList<String>();
        if (multiCommandStreams)
        {
            for (var cmdName: possibleCommandNames)
                topicList.add(EventUtils.getCommandDataTopicID(sysUID, cmdName));
        }
        else
        {
            var singleTopic = EventUtils.getCommandDataTopicID(sysUID, taskingParams.getName());
            topicList.add(singleTopic);
        }
        
        // subscribe to all command data topics of the associated procedure
        eventBus.newSubscription(CommandEvent.class)
            .withTopicIDs(topicList)
            .subscribe(new Subscriber<CommandEvent>() {
                
                @Override
                public void onNext(CommandEvent event)
                {
                    var cmd = event.getCommand();
                    DataBlock cmdData;
                    
                    if (multiCommandStreams)
                    {
                        // add commandName to make it work with interleaved choice structure
                        cmdData = new DataBlockMixed(new DataBlockString(1), (AbstractDataBlock)cmd.getParams());
                        cmdData.setStringValue(0, event.getControlInputName());
                    }
                    else
                        cmdData = cmd.getParams();
                    
                    subscriber.onNext(cmdData);
                }
                
                public void onSubscribe(Subscription sub) { subscriber.onSubscribe(sub); }
                public void onError(Throwable e) { subscriber.onError(e); }
                public void onComplete() { subscriber.onComplete(); }
            });
    }


    @Override
    public SPSConnectorConfig getConfig()
    {
        return config;
    }

}
