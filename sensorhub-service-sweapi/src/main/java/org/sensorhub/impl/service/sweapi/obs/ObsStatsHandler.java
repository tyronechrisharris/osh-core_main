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
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStats;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.service.sweapi.BaseHandler;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.impl.system.DataStreamTransactionHandler;


public class ObsStatsHandler extends BaseHandler
{
    static final String READ_ONLY_ERROR = "Statistics is a read-only resource";
    public static final String[] NAMES = { "stats" };
    
    final ObsSystemDbWrapper db;
    IdConverter idConverter;
    final IdEncoder dsIdEncoder = new IdEncoder(DataStreamHandler.EXTERNAL_ID_SEED);
    final IdEncoder foiIdEncoder = new IdEncoder(FoiHandler.EXTERNAL_ID_SEED);
    final ResourcePermissions permissions;
    
    
    static class ObsHandlerContextData
    {
        long dsID;
        IDataStreamInfo dsInfo;
        FeatureId foiId;
        DataStreamTransactionHandler dsHandler;
    }
    
    
    public ObsStatsHandler(ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        this.db = db;
        this.permissions = permissions;
        this.idConverter = db.getIdConverter();
    }
    
    
    protected ResourceBinding<BigInteger, ObsStats> getBinding(RequestContext ctx) throws IOException
    {
        return new ObsStatsBindingJson(ctx);
    }
    
    
    @Override
    public void doGet(final RequestContext ctx) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
        
        // parse offset & limit
        var queryParams = ctx.getParameterMap();
        var offset = parseLongArg("offset", queryParams);
        if (offset == null)
            offset = 0L;
        
        var limit = parseLongArg("limit", queryParams);
        if (limit == null)
            limit = 100L;
        limit = Math.min(limit, 10000);
        
        // create datastore filter
        var query = getFilter(ctx.getParentRef(), queryParams, offset, limit);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        
        var binding = getBinding(ctx);
        binding.startCollection();
        
        // fetch from DB and temporarily handle paging here
        try (var results = db.getObservationStore().getStatistics(query))
        {
            var it = results.skip(offset)
                .limit(limit+1) // get one more so we know when to enable paging
                .iterator();
            
            var count = 0;
            while (it.hasNext())
            {
                if (count++ >= limit)
                    break;
                var rec = it.next();
                binding.serialize(null, rec, false);
            }
            
            binding.endCollection(getPagingLinks(ctx, offset, limit, count > limit));
        }
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation(READ_ONLY_ERROR);
    }
    
    
    @Override
    public void doPut(RequestContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation(READ_ONLY_ERROR);
    }
    
    
    @Override
    public void doDelete(RequestContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation(READ_ONLY_ERROR);
    }


    protected ObsStatsQuery getFilter(ResourceRef parent, Map<String, String[]> queryParams, long offset, long limit) throws InvalidRequestException
    {
        // first build obs filter
        var builder = new ObsFilter.Builder();
        
        // filter on parent if needed
        if (parent.internalID > 0)
            builder.withDataStreams(parent.internalID);
        
        // phenomenonTime param
        var phenomenonTime = parseTimeStampArg("phenomenonTime", queryParams);
        if (phenomenonTime != null)
            builder.withPhenomenonTime(phenomenonTime);
        
        // resultTime param
        var resultTime = parseTimeStampArg("resultTime", queryParams);
        if (resultTime != null)
            builder.withResultTime(resultTime);
        
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
        
        // now build stats query from obs filter
        var queryBuilder = new ObsStatsQuery.Builder()
            .selectObservations(builder.build());
        
        // histogram bin size
        var binSize = getSingleParam("binSize", queryParams);
        if (binSize != null)
        {
            try
            {
                var d = Duration.parse(binSize);
                queryBuilder.withHistogramBinSize(d);
            }
            catch (DateTimeParseException e)
            {
                throw ServiceErrors.badRequest("Invalid bin duration: " + binSize + ". Must be in ISO8601 duration format.");
            }
        }
        
        // limit
        // need to limit to offset+limit+1 since we rescan from the beginning for now
        queryBuilder.withLimit(offset+limit+1);
        
        return queryBuilder.build(); 
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
