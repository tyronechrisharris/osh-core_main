/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.task;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import org.sensorhub.api.command.CommandStatusEvent;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.command.ICommandStatus.CommandStatusCode;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.command.CommandStatusFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.BaseResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.stream.StreamHandler;
import org.sensorhub.impl.system.CommandStreamTransactionHandler;
import org.sensorhub.impl.system.SystemDatabaseTransactionHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.utils.CallbackException;
import org.vast.util.Asserts;


public class CommandStatusHandler extends BaseResourceHandler<BigInteger, ICommandStatus, CommandStatusFilter, ICommandStatusStore>
{
    public static final int EXTERNAL_ID_SEED = 71145893;
    public static final String[] NAMES = { "status" };
    
    final IEventBus eventBus;
    final ObsSystemDbWrapper db;
    final SystemDatabaseTransactionHandler transactionHandler;
    final ScheduledExecutorService threadPool;
    final IdConverter idConverter;
    
    
    static class CommandStatusHandlerContextData
    {
        long streamID;
        ICommandStreamInfo dsInfo;
        long foiId;
        CommandStreamTransactionHandler dsHandler;
    }
    
    
    public CommandStatusHandler(IEventBus eventBus, ObsSystemDbWrapper db, ScheduledExecutorService threadPool, ResourcePermissions permissions)
    {
        super(db.getCommandStatusStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        
        this.eventBus = eventBus;
        this.db = db;
        this.transactionHandler = !db.isReadOnly() ?
            new SystemDatabaseTransactionHandler(eventBus, db.getWriteDb()) : null;
        this.threadPool = threadPool;
        this.idConverter = db.getIdConverter();
    }
    
    
    @Override
    protected ResourceBinding<BigInteger, ICommandStatus> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        var contextData = new CommandStatusHandlerContextData();
        ctx.setData(contextData);
        
        // try to fetch command stream since it's needed to configure binding
        var publicDsID = ctx.getParentID();
        if (publicDsID > 0)
            contextData.dsInfo = db.getCommandStreamStore().get(new CommandStreamKey(publicDsID));
                
        if (forReading)
        {
            // when ingesting status, command stream should be known at this stage
            Asserts.checkNotNull(contextData.dsInfo, ICommandStreamInfo.class);
            
            // create transaction handler here so it can be reused multiple times
            contextData.streamID = idConverter.toInternalID(publicDsID);
            contextData.dsHandler = transactionHandler.getCommandStreamHandler(contextData.streamID);
            if (contextData.dsHandler == null)
                throw ServiceErrors.notWritable();
        }
        
        // select binding depending on format
        if (format.isOneOf(ResourceFormat.JSON))
            return new CommandStatusBindingJson(ctx, idEncoder, forReading, dataStore);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws IOException
    {
        if (ctx.isEndOfPath() &&
            !(ctx.getParentRef().type instanceof CommandStreamHandler))
            throw ServiceErrors.unsupportedOperation("Observations can only be created within a Datastream");
        
        super.doPost(ctx);
    }
    
    
    protected void subscribe(final RequestContext ctx) throws InvalidRequestException, IOException
    {
        ctx.getSecurityHandler().checkPermission(permissions.stream);
        Asserts.checkNotNull(ctx.getStreamHandler(), StreamHandler.class);
        
        var dsID = ctx.getParentID();
        if (dsID <= 0)
            throw ServiceErrors.badRequest("Streaming is only supported on a specific command stream");
        
        var queryParams = ctx.getParameterMap();
        var filter = getFilter(ctx.getParentRef(), queryParams, 0, Long.MAX_VALUE);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        
        // continue when streaming actually starts
        ctx.getStreamHandler().setStartCallback(() -> {
            
            try
            {
                // init binding and get datastream info
                var binding = getBinding(ctx, false);
                startRealTimeStream(ctx, dsID, filter, binding);
            }
            catch (IOException e)
            {
                throw new IllegalStateException("Error initializing binding", e);
            }
        });
    }
    
    
    protected void startRealTimeStream(final RequestContext ctx, final long dsID, final CommandStatusFilter filter, final ResourceBinding<BigInteger, ICommandStatus> binding)
    {
        // init event to obs converter
        var dsInfo = ((CommandStatusHandlerContextData)ctx.getData()).dsInfo;
        var streamHandler = ctx.getStreamHandler();
        
        // create subscriber
        var subscriber = new Subscriber<CommandStatusEvent>() {
            Subscription subscription;
            
            @Override
            public void onSubscribe(Subscription subscription)
            {
                this.subscription = subscription;
                subscription.request(Long.MAX_VALUE);
                ctx.getLogger().debug("Starting real-time command status subscription #{}", System.identityHashCode(subscription));
                
                // cancel subscription if streaming is stopped by client
                ctx.getStreamHandler().setCloseCallback(() -> {
                    subscription.cancel();
                    ctx.getLogger().debug("Cancelling real-time command status subscription #{}", System.identityHashCode(subscription));
                });
            }

            @Override
            public void onNext(CommandStatusEvent event)
            {
                try
                {
                    binding.serialize(null, event.getStatus(), false);
                    streamHandler.sendPacket();
                }
                catch (IOException e)
                {
                    subscription.cancel();
                    throw new CallbackException(e);
                }
            }

            @Override
            public void onError(Throwable e)
            {
                ctx.getLogger().error("Error while publishing real-time command status", e);
            }

            @Override
            public void onComplete()
            {
                ctx.getLogger().debug("Ending real-time command status subscription #{}", System.identityHashCode(subscription));
                streamHandler.close();
            }
        };
        
        var topic = EventUtils.getCommandStatusTopicID(dsInfo);
        eventBus.newSubscription(CommandStatusEvent.class)
            .withTopicID(topic)
            .withEventType(CommandStatusEvent.class)
            .subscribe(subscriber);
    }


    @Override
    protected BigInteger getKey(RequestContext ctx, String id) throws InvalidRequestException
    {
        try
        {
            var internalID = new BigInteger(id, ResourceBinding.ID_RADIX);
            if (internalID.signum() <= 0)
                return null;
            
            return internalID;
        }
        catch (NumberFormatException e)
        {
            throw ServiceErrors.notFound();
        }
    }
    
    
    @Override
    protected String encodeKey(final RequestContext ctx, BigInteger key)
    {
        var externalID = key;
        return externalID.toString(ResourceBinding.ID_RADIX);
    }


    @Override
    protected CommandStatusFilter getFilter(ResourceRef parent, Map<String, String[]> queryParams, long offset, long limit) throws InvalidRequestException
    {
        var builder = new CommandStatusFilter.Builder();
        
        // filter on parent command if needed
        if (parent.bigInternalID != null)
            builder.withCommands(parent.bigInternalID);
        
        // or filter on parent command stream
        else if (parent.internalID > 0)
        {
            builder.withCommands()
                .withCommandStreams(parent.internalID)
                .done();
        }
        
        // command IDs
        var cmdIDs = parseBigResourceIds("commands", queryParams);
        if (parent.bigInternalID == null && cmdIDs != null && !cmdIDs.isEmpty())
        {
            builder.withCommands(cmdIDs);
        }
        
        // reportTime param
        var issueTime = parseTimeStampArg("reportTime", queryParams);
        if (issueTime != null)
            builder.withReportTime(issueTime);
        
        // executionTime param
        var execTime = parseTimeStampArg("executionTime", queryParams);
        if (execTime != null)
            builder.withExecutionTime(execTime);
        
        // statusCode param
        var statusCodes = parseMultiValuesArg("statusCode", queryParams);
        if (statusCodes != null && !statusCodes.isEmpty())
        {
            try
            {
                Set<CommandStatusCode> enumCodes = statusCodes.stream().map(s -> CommandStatusCode.valueOf(s)).collect(Collectors.toSet());
                builder.withStatus(enumCodes);
            }
            catch (Exception e)
            {
                throw ServiceErrors.badRequest("Invalid status code: " + statusCodes);
            }
        }
        
        // limit
        // need to limit to offset+limit+1 since we rescan from the beginning for now
        if (limit != Long.MAX_VALUE)
            builder.withLimit(offset+limit+1);
        
        return builder.build();
    }
    
    
    @Override
    protected ResourceFormat parseFormat(final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var format = super.parseFormat(queryParams);
        
        if (!format.isOneOf(
              ResourceFormat.JSON,
              ResourceFormat.OM_JSON,
              ResourceFormat.OM_XML,
              ResourceFormat.SWE_JSON,
              ResourceFormat.SWE_TEXT,
              ResourceFormat.SWE_XML,
              ResourceFormat.SWE_BINARY,
              ResourceFormat.TEXT_PLAIN,
              ResourceFormat.TEXT_CSV
            ))
            throw ServiceErrors.unsupportedFormat(format);
        
        return format;
    }


    @Override
    protected BigInteger addEntry(RequestContext ctx, ICommandStatus status) throws DataStoreException
    {
        var dsHandler = ((CommandStatusHandlerContextData)ctx.getData()).dsHandler;
        var id = dsHandler.sendStatus(ctx.getCorrelationID(), status);
        return idConverter.toPublicID(id);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return false;
    }


    @Override
    protected void validate(ICommandStatus resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
