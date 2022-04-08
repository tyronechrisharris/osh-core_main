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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.Collectors;
import java.util.concurrent.ScheduledExecutorService;
import org.sensorhub.api.command.CommandEvent;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.command.ICommandStatus.CommandStatusCode;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStatusFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.resource.BaseResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.stream.StreamHandler;
import org.sensorhub.impl.system.CommandStreamTransactionHandler;
import org.sensorhub.impl.system.SystemDatabaseTransactionHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.utils.CallbackException;
import org.vast.util.Asserts;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;


public class CommandHandler extends BaseResourceHandler<BigInteger, ICommandData, CommandFilter, ICommandStore>
{
    public static final int EXTERNAL_ID_SEED = 71145893;
    public static final String[] NAMES = { "commands" };
    
    final IEventBus eventBus;
    final IObsSystemDatabase db;
    final SystemDatabaseTransactionHandler transactionHandler;
    final ScheduledExecutorService threadPool;
    final IdConverter idConverter;
    final IdEncoder dsIdEncoder = new IdEncoder(CommandStreamHandler.EXTERNAL_ID_SEED);
    final IdEncoder foiIdEncoder = new IdEncoder(FoiHandler.EXTERNAL_ID_SEED);
    
    
    static class CommandHandlerContextData
    {
        long streamID;
        ICommandStreamInfo dsInfo;
        long foiId;
        CommandStreamTransactionHandler dsHandler;
    }
    
    
    public CommandHandler(IEventBus eventBus, ObsSystemDbWrapper db, ScheduledExecutorService threadPool, ResourcePermissions permissions)
    {
        super(db.getReadDb().getCommandStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        
        this.eventBus = eventBus;
        this.db = db.getReadDb();
        this.threadPool = threadPool;
        this.idConverter = db.getIdConverter();
        
        // I know the doc says otherwise but we need to use the federated DB for command transactions here
        // because we don't write to DB directly but rather send commands to systems that can be in other databases
        this.transactionHandler = new SystemDatabaseTransactionHandler(eventBus, db.getReadDb(), db.getDatabaseRegistry());
    }
    
    
    /* need to override this method since we use BigInteger ids for commands */
    protected IResourceHandler getSubResource(RequestContext ctx, String id) throws InvalidRequestException
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return null;
        
        // decode internal ID for nested resource
        var internalID = decodeBigID(ctx, id);
        
        // check that resource ID valid
        if (!db.getCommandStore().containsKey(internalID))
            throw ServiceErrors.notFound(id);
                
        ctx.setParent(this, internalID);
        return resource;
    }
    
    
    @Override
    protected ResourceBinding<BigInteger, ICommandData> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        var contextData = new CommandHandlerContextData();
        ctx.setData(contextData);
        
        // try to fetch command stream since it's needed to configure binding
        var publicDsID = ctx.getParentID();
        if (publicDsID > 0)
            contextData.dsInfo = db.getCommandStreamStore().get(new CommandStreamKey(publicDsID));
                
        if (forReading)
        {
            // when ingesting commands, datastream should be known at this stage
            Asserts.checkNotNull(contextData.dsInfo, ICommandStreamInfo.class);
            
            // create transaction handler here so it can be reused multiple times
            contextData.streamID = publicDsID;
            contextData.dsHandler = transactionHandler.getCommandStreamHandler(publicDsID, true);
            if (contextData.dsHandler == null)
                throw ServiceErrors.notWritable();
            
            // try to parse featureOfInterest argument
            String foiArg = ctx.getParameter("foi");
            if (foiArg != null)
            {
                long publicFoiID = decodeID(ctx, foiArg);
                if (!db.getFoiStore().contains(publicFoiID))
                    throw ServiceErrors.badRequest("Invalid FOI ID");
                contextData.foiId = idConverter.toInternalID(publicFoiID);
            }
        }
        
        // select binding depending on format
        if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON))
            return new CommandBindingJson(ctx, idEncoder, forReading, dataStore);
        else
            return new CommandBindingSweCommon(ctx, idEncoder, forReading, dataStore);
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
    
    
    protected void startRealTimeStream(final RequestContext ctx, final long dsID, final CommandFilter filter, final ResourceBinding<BigInteger, ICommandData> binding)
    {
        // prepare lazy loaded map of FOI UID to full FeatureId
        var foiIdCache = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Long>() {
                @Override
                public Long load(String uid) throws Exception
                {
                    var fk = db.getFoiStore().getCurrentVersionKey(uid);
                    return fk.getInternalID();
                }
            });
        
        // init event to obs converter
        var dsInfo = ((CommandHandlerContextData)ctx.getData()).dsInfo;
        var streamHandler = ctx.getStreamHandler();
        
        // create subscriber
        var subscriber = new Subscriber<CommandEvent>() {
            Subscription subscription;
            
            @Override
            public void onSubscribe(Subscription subscription)
            {
                this.subscription = subscription;
                subscription.request(Long.MAX_VALUE);
                ctx.getLogger().debug("Starting real-time command subscription #{}", System.identityHashCode(subscription));
                
                // cancel subscription if streaming is stopped by client
                ctx.getStreamHandler().setCloseCallback(() -> {
                    subscription.cancel();
                    ctx.getLogger().debug("Cancelling real-time command subscription #{}", System.identityHashCode(subscription));
                });
            }

            @Override
            public void onNext(CommandEvent event)
            {
                try
                {
                    binding.serialize(null, event.getCommand(), false);
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
                ctx.getLogger().error("Error while publishing real-time command data", e);
            }

            @Override
            public void onComplete()
            {
                ctx.getLogger().debug("Ending real-time command subscription #{}", System.identityHashCode(subscription));
                streamHandler.close();
            }
        };
        
        var topic = EventUtils.getCommandDataTopicID(dsInfo);
        eventBus.newSubscription(CommandEvent.class)
            .withTopicID(topic)
            .withEventType(CommandEvent.class)
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
    protected CommandFilter getFilter(ResourceRef parent, Map<String, String[]> queryParams, long offset, long limit) throws InvalidRequestException
    {
        var builder = new CommandFilter.Builder();
        
        // filter on parent if needed
        if (parent.internalID > 0)
            builder.withCommandStreams(parent.internalID);
        
        // issueTime param
        var issueTime = parseTimeStampArg("issueTime", queryParams);
        if (issueTime != null)
            builder.withIssueTime(issueTime);
        
        // status filter params
        var statusCodes = parseMultiValuesArg("statusCode", queryParams);
        var execTime = parseTimeStampArg("executionTime", queryParams);
        if (execTime != null || !statusCodes.isEmpty())
        {
            var statusFilter = new CommandStatusFilter.Builder();
            
            // executionTime
            if (execTime != null)
                statusFilter.withExecutionTime(execTime);
            
            // statusCodes
            if (!statusCodes.isEmpty())
            {
                try
                {
                    Set<CommandStatusCode> enumCodes = statusCodes.stream().map(s -> CommandStatusCode.valueOf(s)).collect(Collectors.toSet());
                    statusFilter.withStatus(enumCodes)
                                .latestReport();
                }
                catch (Exception e)
                {
                    throw ServiceErrors.badRequest("Invalid status code: " + statusCodes);
                }
            }
            
            builder.withStatus(statusFilter.build());
        }
        
        /*// foi param
        var foiIDs = parseResourceIds("foi", queryParams, foiIdEncoder);
        if (foiIDs != null && !foiIDs.isEmpty())
            builder.withFois(foiIDs);*/
        
        // command stream param
        var dsIDs = parseResourceIds("stream", queryParams, dsIdEncoder);
        if (dsIDs != null && !dsIDs.isEmpty())
            builder.withCommandStreams(dsIDs);
        
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
    protected BigInteger addEntry(RequestContext ctx, ICommandData cmd) throws DataStoreException
    {
        try
        {
            var corrID = ctx.getCorrelationID();
            if (corrID == 0)
                corrID = (int)(Math.random()*1e9);
            
            var dsHandler = ((CommandHandlerContextData)ctx.getData()).dsHandler;
            ICommandStatus status = dsHandler.submitCommand(corrID, cmd)
                .get(10, TimeUnit.SECONDS);
            return idConverter.toPublicID(status.getCommandID());
        }
        catch (TimeoutException e)
        {
            throw new DataStoreException("Timeout before command was acknowledged by receiving system", e);
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e.getMessage(), e.getCause());
        }
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return false;
    }


    @Override
    protected void validate(ICommandData resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
