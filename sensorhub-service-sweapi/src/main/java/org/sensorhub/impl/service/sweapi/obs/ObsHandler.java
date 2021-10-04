/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.procedure.DataEventToObsConverter;
import org.sensorhub.impl.procedure.DataStreamTransactionHandler;
import org.sensorhub.impl.procedure.ProcedureObsTransactionHandler;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.resource.BaseResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.stream.StreamHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.utils.CallbackException;
import org.vast.util.Asserts;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;


public class ObsHandler extends BaseResourceHandler<BigInteger, IObsData, ObsFilter, IObsStore>
{
    public static final int EXTERNAL_ID_SEED = 71145893;
    public static final String[] NAMES = { "observations" };
    
    final IEventBus eventBus;
    final ProcedureObsDbWrapper db;
    final ProcedureObsTransactionHandler transactionHandler;
    final IdConverter idConverter;
    final IdEncoder dsIdEncoder = new IdEncoder(DataStreamHandler.EXTERNAL_ID_SEED);
    final IdEncoder foiIdEncoder = new IdEncoder(FoiHandler.EXTERNAL_ID_SEED);
    
    
    static class ObsHandlerContextData
    {
        long dsID;
        IDataStreamInfo dsInfo;
        long foiId;
        DataStreamTransactionHandler dsHandler;
    }
    
    
    public ObsHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getObservationStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        
        this.eventBus = eventBus;
        this.db = db;
        this.transactionHandler = !db.isReadOnly() ?
            new ProcedureObsTransactionHandler(eventBus, db.getWriteDb()) : null;
        this.idConverter = db.getIdConverter();
    }
    
    
    @Override
    protected ResourceBinding<BigInteger, IObsData> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        var contextData = new ObsHandlerContextData();
        ctx.setData(contextData);
        
        // try to fetch datastream since it's needed to configure binding
        var publicDsID = ctx.getParentID();
        if (publicDsID > 0)
            contextData.dsInfo = db.getDataStreamStore().get(new DataStreamKey(publicDsID));
                
        if (forReading)
        {
            // when ingesting obs, datastream should be known at this stage
            Asserts.checkNotNull(contextData.dsInfo, IDataStreamInfo.class);
            
            // create transaction handler here so it can be reused multiple times
            contextData.dsID = idConverter.toInternalID(publicDsID);
            contextData.dsHandler = transactionHandler.getDataStreamHandler(contextData.dsID);
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
        if (format.isOneOf(ResourceFormat.JSON))
            return new ObsBindingOmJson(ctx, idEncoder, forReading, dataStore);
        else
            return new ObsBindingSweCommon(ctx, idEncoder, forReading, dataStore);
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws IOException
    {
        if (ctx.isEndOfPath() &&
            !(ctx.getParentRef().type instanceof DataStreamHandler))
            throw ServiceErrors.unsupportedOperation("Observations can only be created within a Datastream");
        
        super.doPost(ctx);
    }
    
    
    protected void subscribe(final RequestContext ctx) throws InvalidRequestException, IOException
    {
        ctx.getSecurityHandler().checkPermission(permissions.stream);
        var streamHandler = Asserts.checkNotNull(ctx.getStreamHandler(), StreamHandler.class);
        
        var dsID = ctx.getParentID();
        if (dsID <= 0)
            throw ServiceErrors.badRequest("Streaming is only supported on a specific datastream");
        
        var queryParams = ctx.getParameterMap();
        var filter = getFilter(ctx.getParentRef(), queryParams, 0, Long.MAX_VALUE);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        var binding = getBinding(ctx, false);
        
        // continue when streaming actually starts
        ctx.getStreamHandler().setStartCallback(() -> {
                        
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
            
            // get datastream info and init event to obs converter
            var dsInfo = ((ObsHandlerContextData)ctx.getData()).dsInfo;
            var obsConverter = new DataEventToObsConverter(dsID, dsInfo, uid -> foiIdCache.getUnchecked(uid));
            
            // create subscriber
            var subscriber = new Subscriber<DataEvent>() {
                Subscription subscription;
                
                @Override
                public void onSubscribe(Subscription subscription)
                {
                    this.subscription = subscription;
                    subscription.request(Long.MAX_VALUE);
                    ctx.getLogger().debug("Starting obs subscription " + System.identityHashCode(subscription));
                                        
                    // cancel subscription if streaming is stopped by client
                    ctx.getStreamHandler().setCloseCallback(() -> {
                        subscription.cancel();
                        ctx.getLogger().debug("Cancelling obs subscription " + System.identityHashCode(subscription));
                    });
                }
    
                @Override
                public void onNext(DataEvent item)
                {
                    obsConverter.toObs(item, obs -> {
                        try
                        {
                            binding.serialize(null, obs, false);
                            streamHandler.sendPacket();
                        }
                        catch (IOException e)
                        {
                            subscription.cancel();
                            throw new CallbackException(e);
                        } 
                    });
                }
    
                @Override
                public void onError(Throwable e)
                {
                    ctx.getLogger().error("Error while publishing obs data", e);
                }
    
                @Override
                public void onComplete()
                {
                    try { streamHandler.getOutputStream().close(); }
                    catch (IOException e) { }
                }
            };
            
            var topic = EventUtils.getDataStreamDataTopicID(dsInfo);
            eventBus.newSubscription(DataEvent.class)
                .withTopicID(topic)
                .withEventType(DataEvent.class)
                .subscribe(subscriber);
        });
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
    protected ObsFilter getFilter(ResourceRef parent, Map<String, String[]> queryParams, long offset, long limit) throws InvalidRequestException
    {
        var builder = new ObsFilter.Builder();
        
        // filter on parent if needed
        if (parent.internalID > 0)
            builder.withDataStreams(parent.internalID);
        
        // phenomenonTime param
        var phenomenonTime = parseTimeStampArg("phenomenonTime", queryParams);
        if (phenomenonTime != null)
        {
            builder.withPhenomenonTime(new TemporalFilter.Builder()
                .fromTimeExtent(phenomenonTime)
                .build());
        }
        
        // resultTime param
        var resultTime = parseTimeStampArg("resultTime", queryParams);
        if (resultTime != null)
        {
            builder.withResultTime(new TemporalFilter.Builder()
                .fromTimeExtent(resultTime)
                .build());
        }
        
        // foi param
        var foiIDs = parseResourceIds("foi", queryParams, foiIdEncoder);
        if (foiIDs != null && !foiIDs.isEmpty())
            builder.withFois(foiIDs);
        
        // datastream param
        var dsIDs = parseResourceIds("datastream", queryParams, dsIdEncoder);
        if (dsIDs != null && !dsIDs.isEmpty())
            builder.withDataStreams(dsIDs);
        
        // use opensearch bbox param to filter spatially
        var bbox = parseBboxArg("bbox", queryParams);
        if (bbox != null)
        {
            builder.withPhenomenonLocation(new SpatialFilter.Builder()
                .withBbox(bbox)
                .build());
        }
        
        // geom param
        var geom = parseGeomArg("location", queryParams);
        if (geom != null)
        {
            builder.withPhenomenonLocation(new SpatialFilter.Builder()
                .withRoi(geom)
                .build());
        }
        
        // limit
        // need to limit to offset+limit+1 since we rescan from the beginning for now
        builder.withLimit(offset+limit+1);
        
        return builder.build();
    }


    @Override
    protected BigInteger addEntry(RequestContext ctx, IObsData res) throws DataStoreException
    {
        var dsHandler = ((ObsHandlerContextData)ctx.getData()).dsHandler;
        return idConverter.toPublicID(dsHandler.addObs(res));
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return false;
    }


    @Override
    protected void validate(IObsData resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
