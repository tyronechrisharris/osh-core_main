/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.FeatureFilterBase.FeatureFilterBaseBuilder;
import org.sensorhub.impl.service.sweapi.ResourceContext.ResourceRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.ogc.gml.IFeature;


public abstract class AbstractFeatureHistoryHandler<
        V extends IFeature,
        F extends FeatureFilterBase<? super V>,
        B extends FeatureFilterBaseBuilder<B,? super V,F>,
        S extends IFeatureStoreBase<V,?,F>> 
    extends ResourceHandler<FeatureKey, V, F, B, S>
{
    static final Logger log = LoggerFactory.getLogger(AbstractFeatureHistoryHandler.class);
    public static final String NAME = "history";
    static final String VERSION_NOT_FOUND_ERROR_MSG = "Resource version not found: v%s";
    
    
    public AbstractFeatureHistoryHandler(S dataStore, ResourceType<FeatureKey, V> resourceType)
    {
        super(dataStore, resourceType);
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        return ctx.sendError(405, "Cannot POST in feature history, use PUT on main resource URL with a new validTime");
    }
    
    
    @Override
    public boolean doPut(ResourceContext ctx) throws IOException
    {
        return ctx.sendError(405, "Cannot PUT in feature history, use PUT on main resource URL with a new validTime");
    }
    
    
    @Override
    protected boolean getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // internal ID & version number
        long internalID = ctx.getParentID();
        long version = getVersionNumber(ctx, id);
        if (version < 0)
            return false;
        
        var key = getKey(internalID, version);
        V res = dataStore.get(key);
        if (res == null)
            return ctx.sendError(404, String.format(VERSION_NOT_FOUND_ERROR_MSG, id));
        
        var queryParams = ctx.req.getParameterMap();
        ctx.resp.setStatus(200);
        resourceType.serialize(key, res, parseSelectArg(queryParams), ResourceFormat.GEOJSON, ctx.resp.getOutputStream());
        return true;
    }
    
    
    @Override
    protected boolean delete(final ResourceContext ctx, final String id) throws IOException
    {
        // internal ID & version number
        long internalID = ctx.getParentID();
        long version = getVersionNumber(ctx, id);
        if (version < 0)
            return false;
        
        // delete resource
        IFeature res = dataStore.remove(getKey(internalID, version));
        if (res != null)
            return ctx.sendSuccess(204);
        else
            return ctx.sendError(404, String.format(VERSION_NOT_FOUND_ERROR_MSG, id));
    }
    
    
    protected long getVersionNumber(final ResourceContext ctx, final String id)
    {
        try
        {
            long timeStamp = Long.parseLong(id);
            
            // stop here if version is negative
            if (timeStamp <= 0)
                throw new NumberFormatException();
            
            return timeStamp;
        }
        catch (NumberFormatException e)
        {
            ctx.sendError(400, String.format("Invalid version number: %s", id));
            return -1;
        }
    }
    
    
    @Override
    protected IResourceHandler getSubResource(ResourceContext ctx, String id)
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return null;
        
        long internalID = ctx.getParentRef().internalID;
        long version = getVersionNumber(ctx, id);
        ctx.setParent(resourceType, internalID, version);
        
        return resource;
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final B builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        builder.withInternalIDs(parent.internalID);
        
        // validTime param
        String[] validTime = queryParams.get("validTime");
        if (validTime != null)
        {
            var timeExtent = parseTimeStampArg(validTime);
            builder.withValidTime(new TemporalFilter.Builder()
                .fromTimeExtent(timeExtent)
                .build());
        }
    }


    @Override
    protected FeatureKey getKey(long internalID)
    {
        throw new UnsupportedOperationException();
    }
    
    
    protected FeatureKey getKey(long internalID, long version)
    {
        //var fk = new FeatureKey(internalID, Instant.ofEpochSecond(version));
        
        return dataStore.selectKeys(dataStore.filterBuilder()
                .withInternalIDs(internalID)
                .validAtTime(Instant.ofEpochSecond(version))
                .build())
            .findFirst()
            .orElse(null);
    }
    
    
    @Override
    public String getName()
    {
        return NAME;
    }
}
