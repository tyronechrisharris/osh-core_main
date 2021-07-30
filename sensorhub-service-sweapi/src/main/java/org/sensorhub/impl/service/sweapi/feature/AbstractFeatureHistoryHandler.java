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
import org.sensorhub.impl.service.sweapi.ServiceErrors;
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
    
    public static final String[] NAMES = { "history" };
    
    
    public AbstractFeatureHistoryHandler(S dataStore, IdEncoder idEncoder, ResourcePermissions permissions)
    {
        super(dataStore, idEncoder, permissions);
    }
    
    
    @Override
    public void doPost(ResourceContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation("Cannot POST in history collection, use PUT on main resource URL with a new validTime");
    }
    
    
    @Override
    public void doPut(ResourceContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation("Cannot PUT in history collection, use PUT on main resource URL with a new validTime");
    }
    
    
    @Override
    protected void getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
        
        // internal ID & version number
        long internalID = ctx.getParentID();
        long version = getVersionNumber(ctx, id);
        
        var key = getKey(internalID, version);
        V res = dataStore.get(key);
        if (res == null)
            throw ServiceErrors.notFound();
        
        var queryParams = ctx.getParameterMap();
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        var binding = getBinding(ctx, false);
        
        ctx.setResponseContentType(responseFormat.getMimeType());
        binding.serialize(key, res, true);
    }
    
    
    @Override
    protected void delete(final ResourceContext ctx, final String id) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.delete);
        
        // internal ID & version number
        long internalID = ctx.getParentID();
        long version = getVersionNumber(ctx, id);
        
        // delete resource
        IFeature res = dataStore.remove(getKey(internalID, version));
        if (res == null)
            throw ServiceErrors.notFound();
    }
    
    
    protected long getVersionNumber(final ResourceContext ctx, final String version) throws InvalidRequestException
    {
        try
        {
            long num = Long.parseLong(version);
            
            // stop here if version is negative
            if (num <= 0)
                throw new NumberFormatException();
            
            return num;
        }
        catch (NumberFormatException e)
        {
            throw ServiceErrors.badRequest(INVALID_VERSION_ERROR_MSG + version);
        }
    }
    
    
    @Override
    protected IResourceHandler getSubResource(ResourceContext ctx, String id) throws InvalidRequestException
    {
        IResourceHandler resource = getSubResource(ctx);
        
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
