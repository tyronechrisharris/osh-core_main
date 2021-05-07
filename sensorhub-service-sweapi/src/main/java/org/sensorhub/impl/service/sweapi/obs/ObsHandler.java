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
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.impl.procedure.DataStreamTransactionHandler;
import org.sensorhub.impl.procedure.ProcedureObsTransactionHandler;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.resource.BaseResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.util.Asserts;


public class ObsHandler extends BaseResourceHandler<BigInteger, IObsData, ObsFilter, IObsStore>
{
    public static final int EXTERNAL_ID_SEED = 71145893;
    public static final String[] NAMES = { "observations", "obs" };
    
    IEventBus eventBus;
    ProcedureObsDbWrapper db;
    ProcedureObsTransactionHandler transactionHandler;
    IdConverter idConverter;
    IdEncoder dsIdEncoder = new IdEncoder(DataStreamHandler.EXTERNAL_ID_SEED);
    IdEncoder foiIdEncoder = new IdEncoder(FoiHandler.EXTERNAL_ID_SEED);
    
    
    static class ObsHandlerContextData
    {
        long dsID;
        IDataStreamInfo dsInfo;
        FeatureId foiId;
        DataStreamTransactionHandler dsHandler;
    }
    
    
    public ObsHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getObservationStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        
        this.eventBus = eventBus;
        this.db = db;
        this.transactionHandler = new ProcedureObsTransactionHandler(eventBus, db.getWriteDb());
        this.idConverter = db.getIdConverter();
    }
    
    
    @Override
    protected ResourceBinding<BigInteger, IObsData> getBinding(ResourceContext ctx, boolean forReading) throws IOException
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
            
            // try to parse featureOfInterest argument
            String foiArg = ctx.getRequest().getParameter("foi");
            if (foiArg != null)
            {
                long publicFoiID = decodeID(ctx, foiArg);
                var foi = db.getFoiStore().getCurrentVersion(publicFoiID);
                if (foi == null)
                    throw new InvalidRequestException("Invalid FOI ID");
                contextData.foiId = new FeatureId(
                    idConverter.toInternalID(publicFoiID),
                    foi.getUniqueIdentifier());
            }
        }
        
        // select binding depending on format
        if (format.isOneOf(ResourceFormat.JSON))
            return new ObsBindingOmJson(ctx, idEncoder, forReading, dataStore);
        else
            return new ObsBindingSweCommon(ctx, idEncoder, forReading, dataStore);
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEmpty() &&
            !(ctx.getParentRef().type instanceof DataStreamHandler))
            return ctx.sendError(405, "Observations can only be created within a Datastream");
        
        return super.doPost(ctx);
    }
    
    
    protected boolean stream(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        ctx.getSecurityHandler().checkPermission(permissions.stream);
        
        var queryParams = ctx.getRequest().getParameterMap();
        var filter = getFilter(ctx.getParentRef(), queryParams, 0, Long.MAX_VALUE);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        
        try
        {
            ctx.getWebsocketFactory().acceptWebSocket(new WebSocketCreator() {
                @Override
                public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp)
                {
                    try
                    {
                        if (req.getSubProtocols().contains("ingest"))
                        {
                            // get binding for parsing incoming obs records
                            var binding = getBinding(ctx, true);                   
                            return null;
                        }
                        else
                            return new ObsWebSocketOut(ObsHandler.this, ctx);
                        }
                    catch (IOException e)
                    {
                        throw new IllegalStateException("Error handling websocket request", e);
                    }
                }
            }, ctx.getRequest(), ctx.getResponse());
            
            return true;
        }
        catch (Exception e)
        {
            String errorMsg = "Error while processing Websocket request";
            ctx.getLogger().trace(errorMsg, e);
            return ctx.sendError(400, errorMsg);
        }
    }


    @Override
    protected BigInteger getKey(ResourceContext ctx, String id)
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
            ctx.sendError(400, String.format("Invalid resource identifier: %s", id));
            return null;
        }
    }
    
    
    @Override
    protected String encodeKey(final ResourceContext ctx, BigInteger key)
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
    protected BigInteger addEntry(ResourceContext ctx, IObsData res) throws DataStoreException
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
