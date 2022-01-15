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
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.sensorhub.api.command.CommandEvent;
import org.sensorhub.api.command.ICommandAck;
import org.sensorhub.api.command.ICommandAck.CommandStatusCode;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.service.ServiceException;
import org.sensorhub.impl.system.CommandStreamTransactionHandler;
import org.sensorhub.impl.system.SystemUtils;
import org.vast.data.AbstractDataBlock;
import org.vast.data.DataBlockList;
import org.vast.data.DataBlockMixed;
import org.vast.data.DataBlockString;
import org.vast.data.SWEFactory;
import org.vast.ows.sps.StatusReport.TaskStatus;
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
    
    
    public SystemTaskingConnector(final SPSService service, final SystemTaskingConnectorConfig config)
    {
        this.servlet = Asserts.checkNotNull(service.getServlet(), SPSServlet.class);
        this.config = Asserts.checkNotNull(config, SPSConnectorConfig.class);
        this.sysUID = Asserts.checkNotNullOrEmpty(config.systemUID, "sysUID");
        this.db = Asserts.checkNotNull(service.getReadDatabase(), IObsSystemDatabase.class);
        this.eventBus = service.getParentHub().getEventBus();
    }
    
    
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
    public void submitTask(ITask task) throws SensorHubException
    {
        var sysUID = task.getRequest().getProcedureID();
        getCommandNames(task.getRequest().getParameters().getElementType());
        
        try
        {
            DataBlockList dataBlockList = (DataBlockList)task.getRequest().getParameters().getData();
            var it = dataBlockList.blockIterator();
            while (it.hasNext())
            {
                var data = it.next();
                sendCommand(data, ack -> {
                    synchronized (task)
                    {
                        if (ack.getStatusCode() == CommandStatusCode.COMPLETED)
                        {
                            task.getStatusReport().setTaskStatus(TaskStatus.Completed);
                            if (ack.getMessage() != null)
                                task.getStatusReport().setStatusMessage(ack.getMessage());
                            task.getStatusReport().setPercentCompletion(100.f);
                        }
                        else
                        {
                            task.getStatusReport().setTaskStatus(TaskStatus.Failed);
                            if (ack.getMessage() != null)
                                task.getStatusReport().setStatusMessage(ack.getMessage());
                            else if (ack.getError() != null)
                                task.getStatusReport().setStatusMessage(ack.getError().getMessage());
                        }
                    }
                });
            }
        }
        catch (Exception e)
        {
            String msg = "Error sending command to " + sysUID;
            throw new ServiceException(msg, e);
        }
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
    public void sendCommand(DataBlock data, Consumer<ICommandAck> ackCallback)
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
        
        txnHandler.sendCommand(data, ackCallback);
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
                topicList.add(EventUtils.getCommandStreamDataTopicID(sysUID, cmdName));
        }
        else
        {
            var singleTopic = EventUtils.getCommandStreamDataTopicID(sysUID, taskingParams.getName());
            topicList.add(singleTopic);
        }
        
        // subscribe to all command data topics of the associated procedure
        eventBus.newSubscription(CommandEvent.class)
            .withTopicIDs(topicList)
            .subscribe(new Subscriber<CommandEvent>() {
                
                @Override
                public void onNext(CommandEvent item)
                {
                    for (var cmd: item.getCommands())
                    {
                        DataBlock cmdData;
                        
                        if (multiCommandStreams)
                        {
                            // add commandName to make it work with interleaved choice structure
                            cmdData = new DataBlockMixed(new DataBlockString(1), (AbstractDataBlock)cmd.getParams());
                            cmdData.setStringValue(0, item.getControlInputName());
                        }
                        else
                            cmdData = cmd.getParams();
                        
                        subscriber.onNext(cmdData);
                    }
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
