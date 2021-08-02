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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.IDataStore;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.impl.service.sweapi.BaseHandler;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import org.vast.util.TimeExtent;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;


/**
 * <p>
 * Base resource handler, maintaining connection to a datastore
 * </p>
 * 
 * @param <K> 
 * @param <V> 
 * @param <S> 
 * @param <F> 
 *
 * @author Alex Robin
 * @date Nov 15, 2018
 */
public abstract class BaseResourceHandler<K, V, F extends IQueryFilter, S extends IDataStore<K, V, ?, F>> extends BaseHandler implements IResourceHandler
{
    public static final String INVALID_VERSION_ERROR_MSG = "Invalid version number: ";
    public static final String ALREADY_EXISTS_ERROR_MSG = "Resource already exists";
    public static final String UNSUPPORTED_WEBSOCKET_MSG = "Websocket streaming not supported on this resource";
    
    protected final S dataStore;
    protected final IdEncoder idEncoder;
    protected final ResourcePermissions permissions;
            
    
    public BaseResourceHandler(S dataStore, IdEncoder idEncoder, ResourcePermissions permissions)
    {
        this.dataStore = Asserts.checkNotNull(dataStore, IDataStore.class);
        this.idEncoder = Asserts.checkNotNull(idEncoder, IdEncoder.class);
        this.permissions = Asserts.checkNotNull(permissions, ResourcePermissions.class);
    }
    
    
    protected abstract ResourceBinding<K, V> getBinding(ResourceContext ctx, boolean forReading) throws IOException;
    protected abstract K getKey(final ResourceContext ctx, final String id) throws InvalidRequestException;
    protected abstract String encodeKey(final ResourceContext ctx, K key);
    protected abstract F getFilter(final ResourceRef parent, final Map<String, String[]> queryParams, long offset, long limit) throws InvalidRequestException;
    protected abstract K addEntry(final ResourceContext ctx, final V res) throws DataStoreException;
    protected abstract boolean isValidID(long internalID);
    protected abstract void validate(V resource) throws ResourceParseException;
        
    
    @Override
    public void doGet(final ResourceContext ctx) throws IOException
    {
        // if requesting from this resource collection
        if (ctx.isEndOfPath())
        {
            if (ctx.isStreamRequest())
                stream(ctx);
            else
                list(ctx);
            return;
        }
        
        // otherwise there should be a specific resource ID or 'count'
        String id = ctx.popNextPathElt();
        if (ctx.isEndOfPath())
        {
            if ("count".equals(id))
                getElementCount(ctx);
            else
                getById(ctx, id);
            return;
        }
        
        // next should be nested resource
        IResourceHandler resource = getSubResource(ctx, id);
        if (resource != null)
            resource.doGet(ctx);
        else
            throw ServiceErrors.badRequest(INVALID_URI_ERROR_MSG);
    }
    
    
    @Override
    public void doPost(final ResourceContext ctx) throws IOException
    {
        if (!ctx.isEndOfPath())
        {        
            // next should be resource ID
            String id = ctx.popNextPathElt();
            if (ctx.isEndOfPath())
                throw ServiceErrors.unsupportedOperation("Can only POST on collections");
            
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                resource.doPost(ctx);
            else
                throw ServiceErrors.badRequest(INVALID_URI_ERROR_MSG);
        }
        else
            create(ctx);
    }
    
    
    @Override
    public void doPut(final ResourceContext ctx) throws IOException
    {
        if (ctx.isEndOfPath())
            throw ServiceErrors.unsupportedOperation("Can only PUT on specific resource");
     
        // next should be resource ID
        String id = ctx.popNextPathElt();
        
        if (!ctx.isEndOfPath())
        {
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                resource.doPut(ctx);
            else
                throw ServiceErrors.badRequest(INVALID_URI_ERROR_MSG);
        }
        else
            update(ctx, id);
    }
    
    
    @Override
    public void doDelete(final ResourceContext ctx) throws IOException
    {
        if (ctx.isEndOfPath())
            throw ServiceErrors.unsupportedOperation("Can only DELETE a specific resource");
     
        // next should be resource ID
        String id = ctx.popNextPathElt();
        
        if (!ctx.isEndOfPath())
        {
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                resource.doDelete(ctx);
            else
                throw ServiceErrors.badRequest(INVALID_URI_ERROR_MSG);
        }
        else
            delete(ctx, id);
    }
    
    
    protected void stream(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        throw ServiceErrors.unsupportedOperation(UNSUPPORTED_WEBSOCKET_MSG);
    }
    
    
    protected void getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
                
        // get resource key
        var key = getKey(ctx, id);
        getByKey(ctx, key);
    }
    
    
    protected void getByKey(final ResourceContext ctx, K key) throws InvalidRequestException, IOException
    {
        // fetch from data store
        final V res = dataStore.get(key);
        if (res != null)
        {            
            var queryParams = ctx.getParameterMap();
            var responseFormat = parseFormat(queryParams);
            ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
            var binding = getBinding(ctx, false);
            
            // set content type
            ctx.setResponseContentType(responseFormat.getMimeType());
            
            // write a single resource to servlet output
            binding.serialize(key, res, true);
        }
        else
            throw ServiceErrors.notFound();
    }
    
    
    protected void list(final ResourceContext ctx) throws InvalidRequestException, IOException
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
        F filter = getFilter(ctx.getParentRef(), queryParams, offset, limit);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        
        // set content type
        ctx.setResponseContentType(responseFormat.getMimeType());
        
        // stream and serialize all resources to servlet output
        var binding = getBinding(ctx, false);
        binding.startCollection();
        
        // fetch from DB and temporarily handle paging here
        try (var results = dataStore.selectEntries(filter))
        {
            var it = results.skip(offset)
                .limit(limit+1) // get one more so we know when to enable paging
                .iterator();
            
            var count = 0;
            while (it.hasNext())
            {
                if (count++ >= limit)
                    break;
                var e = it.next();
                binding.serialize(e.getKey(), e.getValue(), false);
            }
            
            binding.endCollection(getPagingLinks(ctx, offset, limit, count > limit));
        }
    }
    
    
    protected Collection<ResourceLink> getPagingLinks(final ResourceContext ctx, long offset, long limit, boolean hasMore) throws InvalidRequestException
    {
        var resourcePath = ctx.getApiRootURL() + "/" + getNames()[0];
        var queryParams = ctx.getParameterMap();
        var links = new ArrayList<ResourceLink>();
        
        // prev link
        if (offset > 0)
        {
            var prevOffset = Math.max(0, offset-limit);
            links.add(new ResourceLink.Builder()
                .rel("prev")
                .href(resourcePath + getQueryString(queryParams, prevOffset))
                .type(ctx.getFormat().getMimeType())
                .build());
        }
        
        // next link
        if (hasMore)
        {
            var nextOffset = offset+limit;
            links.add(new ResourceLink.Builder()
                .rel("next")
                .href(resourcePath + getQueryString(queryParams, nextOffset))
                .type(ctx.getFormat().getMimeType())
                .build());
        }
        
        return links;
    }
    
    
    String getQueryString(Map<String, String[]> queryParams, long offset)
    {
        offset = Math.max(0, offset);
        
        var buf = new StringBuilder();
        buf.append('?');
        
        for (var e: queryParams.entrySet())
        {
            if (!"offset".equals(e.getKey()))
            {
                buf.append(e.getKey()).append("=");
                for (var s: e.getValue())
                    buf.append(s).append(',');
                buf.setCharAt(buf.length()-1, '&');
            }
        }
        
        if (offset > 0)
            buf.append("offset=").append(offset);
        else
            buf.setLength(buf.length()-1);
        
        return buf.toString();
    }
    
    
    protected void create(final ResourceContext ctx) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.create);
        
        // read and ingest one or more resources
        int count = 0;
        String url = null;
        try
        {
            // get content type
            var format = ResourceFormat.fromMimeType(ctx.getRequestContentType());
            if (format == null)
                throw ServiceErrors.badRequest("Missing content type");
            ctx.setFormatOptions(format, null);
            
            // parse one or more resource
            V res;
            var binding = getBinding(ctx, true);
            while ((res = binding.deserialize()) != null)
            {
                try
                {
                    count++;
                    validate(res);
                
                    // add resource to datastore
                    K k = addEntry(ctx, res);
                    
                    if (k != null)
                    {
                        // encode resource ID and generate URL
                        String id = encodeKey(ctx, k);
                        url = getCanonicalResourceUrl(id);
                        ctx.getLogger().debug("Added resource {}", url);
                    }                    
                    
                    if (url != null)
                        ctx.addResourceUri(url);
                }
                catch (DataStoreException e)
                {
                    throw ServiceErrors.badRequest(e.getMessage());
                }
                catch (Exception e)
                {
                    throw new IOException("Error ingesting entry", e);
                }
            }
            
            if (count == 0)
                throw ServiceErrors.badRequest("No data provided");
        }
        catch (ResourceParseException e)
        {
            throw ServiceErrors.invalidPayload(e.getMessage());
        }
    }
        
    
    protected void update(final ResourceContext ctx, final String id) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.update);
                
        // get resource key
        var key = getKey(ctx, id);
        
        // parse payload
        V res;
        try
        {
            // get content type
            var format = ResourceFormat.fromMimeType(ctx.getRequestContentType());
            if (format == null)
                throw ServiceErrors.badRequest("Missing content type");
            ctx.setFormatOptions(format, null);
            
            // parse one resource
            var binding = getBinding(ctx, true);
            res = binding.deserialize();
            if (res == null)
                throw ServiceErrors.badRequest("No data provided");
            
            validate(res);
        }
        catch (ResourceParseException e)
        {
            throw ServiceErrors.invalidPayload(e.getMessage());
        }
        
        // update in datastore
        try
        {
            if (updateEntry(ctx, key, res))
                ctx.getLogger().debug("Updated resource {}, key={}", id, key);
        }
        catch (DataStoreException e)
        {
            throw new IOException("Error updating resource " + id, e);
        }
    }
    
    
    protected boolean updateEntry(final ResourceContext ctx, final K key, final V res) throws DataStoreException
    {        
        return dataStore.replace(key, res) != null;
    }
        
    
    protected void delete(final ResourceContext ctx, final String id) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.delete);
        
        // get resource key
        var key = getKey(ctx, id);
        
        // delete from datastore
        try
        {
            if (deleteEntry(ctx, key))
                ctx.getLogger().info("Deleted resource {}, key={}", id, key);
        }
        catch (DataStoreException e)
        {
            throw new IOException("Error deleting resource " + id, e);
        }
    }
    
    
    protected boolean deleteEntry(final ResourceContext ctx, final K key) throws DataStoreException
    {        
        return dataStore.remove(key) != null;
    }
    
    
    protected void getElementCount(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        F filter = getFilter(ctx.getParentRef(), ctx.getParameterMap(), 0, Long.MAX_VALUE);
        
        // get count from datastore
        long count = dataStore.countMatchingEntries(filter);
        
        // write out as json
        Writer writer = new OutputStreamWriter(ctx.getOutputStream());
        writer.write("{\"count\":");
        writer.write(Long.toString(count));
        writer.write('}');
        writer.flush();
    }
    
    
    protected IResourceHandler getSubResource(ResourceContext ctx, String id) throws InvalidRequestException
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return null;
        
        // decode internal ID for nested resource
        long internalID = decodeID(ctx, id);
        
        // check that resource ID valid
        if (!isValidID(internalID))
            throw ServiceErrors.notFound(id);
                
        ctx.setParent(this, internalID);
        return resource;
    }
    
    
    protected String getCanonicalResourceUrl(final String id)
    {
        StringBuilder buf = new StringBuilder();
        buf.append('/').append(getNames()[0]);
        buf.append('/').append(id);
        return buf.toString();
    }
    
    
    protected long decodeID(final ResourceContext ctx, final String id) throws InvalidRequestException
    {
        try
        {
            long encodedID = Long.parseLong(id, ResourceBinding.ID_RADIX);
            long decodedID = idEncoder.decodeID(encodedID);
            
            // stop here if hash is invalid
            if (decodedID <= 0)
                throw ServiceErrors.notFound(id);
            
            return decodedID;
        }
        catch (NumberFormatException e)
        {
            throw ServiceErrors.badRequest("Invalid resource ID: " + id);
        }
    }
    
    
    protected PropertyFilter parseSelectArg(final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var paramValues = queryParams.get("select");
                
        if (paramValues != null)
        {
            var propFilter = new PropertyFilter();
            
            for (String val: paramValues)
            {
                for (String item: val.split(","))
                {
                    item = item.trim();
                    if (item.isEmpty())
                        throw ServiceErrors.badRequest("Invalid select parameter: " + val);
                    
                    else if (item.startsWith("!"))
                        propFilter.excludedProps.add(item.substring(1));
                    else
                        propFilter.includedProps.add(item);
                }
                
                if (propFilter.includedProps.isEmpty() && propFilter.excludedProps.isEmpty())
                    throw ServiceErrors.badRequest("Invalid select parameter: " + val);
            }
            
            return propFilter;
        }
        
        return null;
    }
    
    
    protected ResourceFormat parseFormat(final Map<String, String[]> queryParams)
    {
        var format = queryParams.get("f");
        if (format == null)
            format = queryParams.get("format");
        
        ResourceFormat rf = null;
        if (format != null)
            rf = ResourceFormat.fromMimeType(format[0]);
        
        if (rf == null)
            rf = ResourceFormat.JSON; // defaults to json;
        
        return rf;
    }
    
    
    protected Collection<Long> parseResourceIds(String paramName, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        return parseResourceIds(paramName, queryParams, this.idEncoder);
    }
    
        
    protected Collection<Long> parseResourceIds(String paramName, final Map<String, String[]> queryParams, IdEncoder idEncoder) throws InvalidRequestException
    {
        var allValues = new ArrayList<Long>();
        
        var paramValues = queryParams.get(paramName);
        if (paramValues != null)
        {
            for (String val: paramValues)
            {
                for (String id: val.split(","))
                {
                    try
                    {
                        long externalID = Long.parseLong(id, ResourceBinding.ID_RADIX);
                        long internalID = idEncoder.decodeID(externalID);
                        allValues.add(internalID);
                    }
                    catch (NumberFormatException e)
                    {
                        throw ServiceErrors.badRequest("Invalid resource ID: " + id);
                    }
                }
            }
        }
        
        return allValues;
    }
    
    
    protected TimeExtent parseTimeStampArg(String paramName, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var timeVal = getSingleParam(paramName, queryParams);
        if (timeVal == null)
            return null;
        
        try
        {
            return TimeExtent.parse(timeVal);
        }
        catch (Exception e)
        {
            throw ServiceErrors.badRequest("Invalid time parameter: " + timeVal);
        }
    }
    
    
    protected Bbox parseBboxArg(String paramName, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var bboxCoords = parseMultiValuesArg(paramName, queryParams);
        if (bboxCoords == null || bboxCoords.isEmpty())
            return null;
        
        try
        {
            Bbox bbox = new Bbox();
            bbox.setMinX(Double.parseDouble(bboxCoords.get(0)));
            bbox.setMinY(Double.parseDouble(bboxCoords.get(1)));
            bbox.setMaxX(Double.parseDouble(bboxCoords.get(2)));
            bbox.setMaxY(Double.parseDouble(bboxCoords.get(3)));
            bbox.checkValid();
            return bbox;
        }
        catch (Exception e)
        {
            throw ServiceErrors.badRequest("Invalid bounding box: " + bboxCoords);
        }
    }
    
    
    protected Geometry parseGeomArg(String paramName, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var wkt = getSingleParam(paramName, queryParams);
        if (wkt == null)
            return null;
        
        try
        {
            return new WKTReader().read(wkt);
        }
        catch (ParseException e)
        {
            throw ServiceErrors.badRequest("Invalid geometry: " + wkt);
        }
    }
    
    
    protected Long parseLongArg(String paramName, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var paramValue = getSingleParam(paramName, queryParams);
        if (paramValue == null)
            return null;
        
        try
        {
            return Long.parseLong(paramValue);
        }
        catch (NumberFormatException e)
        {
            throw ServiceErrors.badRequest("Invalid " + paramName + " parameter: " + paramValue);
        }
    }
    
    
    protected List<String> parseMultiValuesArg(String paramName, final Map<String, String[]> queryParams)
    {
        var allValues = new ArrayList<String>();
        
        var paramValues = queryParams.get(paramName);
        if (paramValues != null)
        {
            for (String val: paramValues)
            {
                for (String item: val.split(","))
                {
                    if (!item.isBlank())
                        allValues.add(item);
                }
            }
        }
        
        return allValues;
    }
    
    
    protected String getSingleParam(String paramName, final Map<String, String[]> queryParams) throws InvalidRequestException
    {
        var paramValues = parseMultiValuesArg(paramName, queryParams);
        
        if (paramValues.size() > 1)
            throw ServiceErrors.badRequest("Parameter '" + paramName + "' must have a single value");
        
        if (paramValues.isEmpty())
            return null;
        
        return paramValues.iterator().next();
    }
}
