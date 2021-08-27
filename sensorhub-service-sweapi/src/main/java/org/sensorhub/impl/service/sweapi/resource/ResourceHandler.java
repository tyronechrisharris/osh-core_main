/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.resource;

import java.util.Map;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.resource.IResourceStore;
import org.sensorhub.api.resource.ResourceFilter;
import org.sensorhub.api.resource.ResourceFilter.ResourceFilterBuilder;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
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
    
    
    protected ResourceHandler(S dataStore, IdEncoder idEncoder, ResourcePermissions permissions)
    {
        super(dataStore, idEncoder, permissions);
    }
    
    
    protected abstract K getKey(long publicID);
    
    
    @Override
    protected K getKey(final RequestContext ctx, final String id) throws InvalidRequestException
    {
        // get resource ID
        long decodedID = decodeID(ctx, id);
        return getKey(decodedID);
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
    public F getFilter(final ResourceRef parent, final Map<String, String[]> queryParams, long offset, long limit) throws InvalidRequestException
    {
        B builder = (B)dataStore.filterBuilder();
        if (queryParams != null)
        {
            buildFilter(parent, queryParams, builder);
            
            // limit
            // need to limit to offset+limit+1 since we rescan from the beginning for now
            builder.withLimit(offset+limit+1);
        }
        return builder.build();
    }
    
    
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final B builder) throws InvalidRequestException
    {
        // keyword search
        var keywords = parseMultiValuesArg("q", queryParams);
        if (keywords != null && !keywords.isEmpty())
            builder.withKeywords(keywords);
    }
    
    
    protected K addEntry(final RequestContext ctx, final V res) throws DataStoreException
    {        
        return dataStore.add(res);
    }
    
    
    @Override
    protected String encodeKey(final RequestContext ctx, K key)
    {
        long externalID = idEncoder.encodeID(key.getInternalID());
        return Long.toString(externalID, ResourceBinding.ID_RADIX);
    }
}
