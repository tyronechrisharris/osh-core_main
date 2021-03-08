/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.feature;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.FeatureFilterBase.FeatureFilterBaseBuilder;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
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
    static final String VERSION_NOT_FOUND_ERROR_MSG = "Resource version not found: v%s";
    public static final String[] NAMES = { "history" };
    
    
    public AbstractFeatureHistoryHandler(S dataStore, IdEncoder idEncoder, ResourcePermissions permissions)
    {
        super(dataStore, idEncoder, permissions);
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        return ctx.sendError(405, "Cannot POST in history collection, use PUT on main resource URL with a new validTime");
    }
    
    
    @Override
    public boolean doPut(ResourceContext ctx) throws IOException
    {
        return ctx.sendError(405, "Cannot PUT in history collection, use PUT on main resource URL with a new validTime");
    }
    
    
    @Override
    protected boolean getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
        
        // internal ID & version number
        long internalID = ctx.getParentID();
        long version = getVersionNumber(ctx, id);
        if (version < 0)
            return false;
        
        var key = getKey(internalID, version);
        V res = dataStore.get(key);
        if (res == null)
            return ctx.sendError(404, String.format(VERSION_NOT_FOUND_ERROR_MSG, id));
        
        var queryParams = ctx.getRequest().getParameterMap();
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        var binding = getBinding(ctx, false);
        
        ctx.getResponse().setStatus(200);
        ctx.getResponse().setContentType(responseFormat.getMimeType());
        binding.serialize(key, res, true);
        
        return true;
    }
    
    
    @Override
    protected boolean delete(final ResourceContext ctx, final String id) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.delete);
        
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
        ctx.setParent(this, internalID, version);
        
        return resource;
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final B builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        builder.withInternalIDs(parent.internalID);
        
        // validTime param
        var validTime = parseTimeStampArg("validTime", queryParams);
        if (validTime != null)
        {
            builder.withValidTime(new TemporalFilter.Builder()
                .fromTimeExtent(validTime)
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
    public String[] getNames()
    {
        return NAMES;
    }
}
