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

import java.util.Map;
import org.sensorhub.api.resource.IResourceStore;
import org.sensorhub.api.resource.ResourceFilter;
import org.sensorhub.api.resource.ResourceFilter.ResourceFilterBuilder;
import org.sensorhub.impl.service.sweapi.ResourceContext.ResourceRef;
import org.sensorhub.api.resource.ResourceKey;
import org.vast.util.IResource;


/**
 * <p>
 * Base resource handler, maintaining connection to a datastore
 * </p>
 *
 * @author Alex Robin
 * @param <K> 
 * @param <V> 
 * @param <F> 
 * @param <B> 
 * @param <S> 
 * @date Nov 15, 2018
 */
public abstract class ResourceHandler<
    K extends ResourceKey<K>,
    V extends IResource,
    F extends ResourceFilter<? super V>,
    B extends ResourceFilterBuilder<B, ? super V, F>,
    S extends IResourceStore<K,V,?,F>> extends BaseResourceHandler<K, V, F, S>
{
    public static final int NO_PARENT = 0;
    
    
    protected ResourceHandler(S dataStore, ResourceType<K, V> resourceType)
    {
        super(dataStore, resourceType);
    }
    
    
    protected abstract K getKey(long internalID);
    
    
    protected K getKey(final ResourceContext ctx, final String id)
    {
        // get resource internal ID
        long internalID = getInternalID(ctx, id);
        if (internalID <= 0)
            return null;
        
        return getKey(internalID);
    }
        
    
    /*protected long getParentInternalID(final ResourceContext ctx, final String id, final HttpServletResponse resp)
    {
        long internalID = getInternalID(ctx, id, resp);
        
        if (internalID > 0 && !dataStore.containsKey(getKey(internalID)))
        {
            ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id), resp);
            return 0;
        }        
            
        return internalID;
    }*/
    
    
    @SuppressWarnings({ "unchecked" })
    public F getFilter(final ResourceRef parent, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        B builder = (B)dataStore.filterBuilder();
        if (queryParams != null)
            buildFilter(parent, queryParams, builder);
        return builder.build();
    }
    
    
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final B builder) throws InvalidRequestException
    {
        String[] paramValues;
        
        // keyword search
        paramValues = queryParams.get("q");
        if (paramValues != null)
        {
            var keywords = parseMultiValuesArg(paramValues);
            builder.withFullText().withKeywords(keywords).done();
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
        
        builder.withLimit(Math.min(maxResults, 300));
    }
    
    
    @Override
    protected String addToDataStore(final ResourceContext ctx, final V resource)
    {
        K k = dataStore.add(resource);
        
        if (k != null)
        {
            long externalID = resourceType.getExternalID(k.getInternalID());
            String id = Long.toString(externalID, ResourceType.ID_RADIX);
            String url = getCanonicalResourceUrl(id, ctx.req);
            ctx.getLogger().info("Added resource {}={}, URL={}", id, k.getInternalID(), url);
            return url;
        }
        
        return null;
    }
}
