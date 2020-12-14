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

import java.math.BigInteger;
import java.util.Map;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.resource.BaseResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;


public class ObsHandler extends BaseResourceHandler<BigInteger, IObsData, ObsFilter, IObsStore>
{
    public static final String[] NAMES = { "obs", "observations" };
    
    
    public ObsHandler(IObsStore dataStore)
    {
        super(dataStore, new ObsResourceType(dataStore));
    }


    @Override
    protected void validate(IObsData resource)
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    protected BigInteger getKey(ResourceContext ctx, String id)
    {
        try
        {
            var externalID = new BigInteger(id, ResourceType.ID_RADIX);
            var internalID = ((ObsResourceType)resourceType).getInternalID(externalID);
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
    protected ObsFilter getFilter(ResourceRef parent, Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var builder = new ObsFilter.Builder();
        String[] paramValues;
        
        // filter on parent if needed
        if (parent.internalID > 0)
            builder.withDataStreams(parent.internalID);
        
        // phenomenonTime param
        paramValues = queryParams.get("phenomenonTime");
        if (paramValues != null)
        {
            var timeExtent = parseTimeStampArg(paramValues);
            builder.withPhenomenonTime(new TemporalFilter.Builder()
                .fromTimeExtent(timeExtent)
                .build());
        }
        
        // resultTime param
        paramValues = queryParams.get("resultTime");
        if (paramValues != null)
        {
            var timeExtent = parseTimeStampArg(paramValues);
            builder.withResultTime(new TemporalFilter.Builder()
                .fromTimeExtent(timeExtent)
                .build());
        }
        
        // foi param
        paramValues = queryParams.get("foi");
        if (paramValues != null)
        {
            var foiIDs = parseResourceIds(paramValues);
            builder.withFois(foiIDs);
        }
        
        // use opensearch bbox param to filter spatially
        paramValues = queryParams.get("bbox");
        if (paramValues != null)
        {
            var bbox = parseBboxArg(paramValues);
            builder.withPhenomenonLocation(new SpatialFilter.Builder()
                .withBbox(bbox)
                .build());
        }
        
        // geom param
        paramValues = queryParams.get("location");
        if (paramValues != null)
        {
            var geom = parseGeomArg(paramValues);
            builder.withPhenomenonLocation(new SpatialFilter.Builder()
                .withRoi(geom)
                .build());
        }
        
        // limit
        paramValues = queryParams.get("limit");
        int maxResults = Integer.MAX_VALUE;
        if (paramValues != null)
        {
            String limit = getSingleParam("limit", paramValues);
            
            try
            {
                maxResults = Integer.parseInt(limit);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidRequestException("Invalid limit parameter: " + limit);
            }
        }            
        builder.withLimit(Math.min(maxResults, 1000));
        
        return builder.build();
    }


    @Override
    protected String addToDataStore(ResourceContext ctx, IObsData resource)
    {
        BigInteger k = dataStore.add(resource);
        
        if (k != null)
        {
            var externalID = ((ObsResourceType)resourceType).getExternalID(k);
            String id = externalID.toString(ResourceType.ID_RADIX);
            String url = getCanonicalResourceUrl(id, ctx.getRequest());
            ctx.getLogger().trace("Added obs {}", id);
            return url;
        }
        
        return null;
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }

}
