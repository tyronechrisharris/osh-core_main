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
import javax.servlet.http.HttpServletRequest;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.IDataStore;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.impl.service.sweapi.BaseHandler;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import org.vast.util.TimeExtent;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonWriter;
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
    public static final String NOT_FOUND_ERROR_MSG = "Resource not found: %s";
    public static final String ALREADY_EXISTS_ERROR_MSG = "Resource already exists";
    public static final String ACCESS_DENIED_ERROR_MSG = "Permission denied";
    public static final String UNSUPPORTED_FORMAT_ERROR_MSG = "Unsupported format: ";
    public static final String UNSUPPORTED_WEBSOCKET_MSG = "Websocket not unsupported on resource ";
    
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
    protected abstract K getKey(final ResourceContext ctx, final String id);
    protected abstract String encodeKey(final ResourceContext ctx, K key);
    protected abstract F getFilter(final ResourceRef parent, final Map<String, String[]> queryParams) throws InvalidRequestException;
    protected abstract K addEntry(final ResourceContext ctx, final V res) throws DataStoreException;
    protected abstract boolean isValidID(long internalID);
    protected abstract void validate(V resource) throws ResourceParseException;
        
    
    @Override
    public boolean doGet(final ResourceContext ctx) throws IOException
    {
        try
        {
            // if requesting from this resource collection
            if (ctx.isEmpty())
            {
                if (ctx.getWebsocketFactory() != null)
                    return stream(ctx);
                else
                    return list(ctx);
            }
            // next should be resource ID or 'count'
            String id = ctx.popNextPathElt();
            if (ctx.isEmpty())
            {
                if ("count".equals(id))
                    return getElementCount(ctx);
                else
                    return getById(ctx, id);
            }
            
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                return resource.doGet(ctx);
            
            return false;
        }
        catch (InvalidRequestException e)
        {
            return ctx.sendError(400, e.getMessage());
        }
        catch (SecurityException e)
        {
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
        }
    }
    
    
    @Override
    public boolean doPost(final ResourceContext ctx) throws IOException
    {
        try
        {
            if (ctx.isEmpty())
                return create(ctx);
         
            // next should be resource ID
            String id = ctx.popNextPathElt();
            if (ctx.isEmpty())
                return ctx.sendError(400, "Can only POST on collections");
            
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                return resource.doPost(ctx);
                
            return false;
        }
        catch (InvalidRequestException e)
        {
            return ctx.sendError(400, e.getMessage());
        }
        catch (SecurityException e)
        {
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
        }
    }
    
    
    @Override
    public boolean doPut(final ResourceContext ctx) throws IOException
    {
        try
        {
            if (ctx.isEmpty())
                return ctx.sendError(400, "Can only PUT on specific resource");
         
            // next should be resource ID
            String id = ctx.popNextPathElt();
            if (ctx.isEmpty())
                return update(ctx, id);
            
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                return resource.doPut(ctx);
                
            return false;
        }
        catch (InvalidRequestException e)
        {
            return ctx.sendError(400, e.getMessage());
        }
        catch (SecurityException e)
        {
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
        }
    }
    
    
    @Override
    public boolean doDelete(final ResourceContext ctx) throws IOException
    {
        try
        {
            if (ctx.isEmpty())
                return ctx.sendError(400, "Can only DELETE a specific resource");
         
            // next should be resource ID
            String id = ctx.popNextPathElt();
            if (ctx.isEmpty())
                return delete(ctx, id);
            
            // next should be nested resource
            IResourceHandler resource = getSubResource(ctx, id);
            if (resource != null)
                return resource.doDelete(ctx);
                
            return false;
        }
        catch (InvalidRequestException e)
        {
            return ctx.sendError(400, e.getMessage());
        }
        catch (SecurityException e)
        {
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
        }
    }
    
    
    protected boolean stream(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        throw new InvalidRequestException(UNSUPPORTED_WEBSOCKET_MSG + ctx.getRequest().getPathInfo());
    }
    
    
    protected boolean getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
                
        // get resource key
        var key = getKey(ctx, id);
        if (key != null)
        {
            // fetch from data store
            final V res = dataStore.get(key);
            if (res != null)
            {            
                var queryParams = ctx.req.getParameterMap();
                var responseFormat = parseFormat(queryParams);
                ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
                var binding = getBinding(ctx, false);
                
                // set HTTP headers
                ctx.resp.setStatus(200);
                ctx.resp.setContentType(responseFormat.getMimeType());
                
                // write a single resource to servlet output
                binding.serialize(key, res, true);
                return true;
            }
        }
        
        return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
    }
    
    
    protected boolean list(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
                
        var queryParams = ctx.req.getParameterMap();
        F filter = getFilter(ctx.getParentRef(), queryParams);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        
        // parse offset & limit
        var offset = parseLongArg("offset", queryParams);
        if (offset == null)
            offset = 0L;
        
        var limit = parseLongArg("limit", queryParams);
        if (limit == null)
            limit = 100L;
        limit = Math.min(limit, 10000);
        
        // set HTTP headers
        ctx.resp.setStatus(200);
        ctx.resp.setContentType(responseFormat.getMimeType());
        
        // stream and serialize all resources to servlet output
        var binding = getBinding(ctx, false);
        binding.startCollection();
        
        // fetch from DB and temporarily handle paging here
        var results = dataStore.selectEntries(filter)
            .skip(offset)
            .limit(limit+1) // get one more so we know when to enable paging
            .iterator();
        
        var count = 0;
        while (results.hasNext())
        {
            if (count++ >= limit)
                break;
            var e = results.next();
            binding.serialize(e.getKey(), e.getValue(), false);
        }
        
        binding.endCollection(getPagingLinks(ctx, offset, limit, count > limit));
        return true;
    }
    
    
    protected Collection<ResourceLink> getPagingLinks(final ResourceContext ctx, long offset, long limit, boolean hasMore) throws InvalidRequestException
    {
        var resourcePath = ctx.getApiRootURL() + "/" + getNames()[0];
        var queryParams = ctx.getRequest().getParameterMap();
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
    
    
    protected boolean create(final ResourceContext ctx) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.create);
        
        // read and ingest one or more resources
        int count = 0;
        String url = null;
        try
        {
            // get content type
            var format = ResourceFormat.fromMimeType(ctx.req.getContentType());
            if (format == null)
                return ctx.sendError(400, "Missing content type");
            ctx.setFormatOptions(format, null);
            
            // prepare to write JSON response
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(ctx.resp.getOutputStream()));
            writer.beginArray();
            
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
                        url = getCanonicalResourceUrl(id, ctx.req);
                        ctx.getLogger().info("Added resource {}, URL={}", id, url);
                    }                    
                    
                    if (url != null)
                    {
                        if (ctx.resp.getStatus() == 200)
                            ctx.resp.setStatus(201);
                        writer.value(url);
                    }                
                    else
                        ctx.writeError("Could not add resource", writer);
                }
                catch (Exception e)
                {
                    ctx.writeError(e.getMessage(), writer);
                }
            }
            
            if (count == 0)
                return ctx.sendError(400, "No data provided");
            else if (url == null)
                ctx.resp.setStatus(400);            
            else if (count == 1)
                ctx.resp.setHeader("Location", url);
            
            writer.endArray();
            writer.flush();
            
            return (url != null);
        }
        catch (ResourceParseException e)
        {
            return ctx.sendError(400, e.getMessage());
        }
    }
        
    
    protected boolean update(final ResourceContext ctx, final String id) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.update);
                
        // get resource key
        var key = getKey(ctx, id);
        
        if (key != null)
        {        
            // first parse resource
            V res;
            try
            {
                // get content type
                var format = ResourceFormat.fromMimeType(ctx.req.getContentType());
                if (format == null)
                    return ctx.sendError(400, "Missing content type");
                ctx.setFormatOptions(format, null);
                
                // parse one resource
                var binding = getBinding(ctx, true);
                res = binding.deserialize();
                if (res == null)
                    return ctx.sendError(400, "No data provided");
                
                validate(res);
            }
            catch (JsonParseException e)
            {
                return ctx.sendError(400, e.getMessage());
            }
            
            // update in datastore
            try
            {
                if (updateEntry(ctx, key, res))
                {
                    ctx.getLogger().info("Updated resource {}, key={}", id, key);
                    return ctx.sendSuccess(204);
                }
            }
            catch (Exception e)
            {
                return ctx.sendError(500, e.getMessage());
            }
        }
        
        return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
    }
    
    
    protected boolean updateEntry(final ResourceContext ctx, final K key, final V res) throws DataStoreException
    {        
        return dataStore.replace(key, res) != null;
    }
        
    
    protected boolean delete(final ResourceContext ctx, final String id) throws IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.delete);
        
        // get resource key
        var key = getKey(ctx, id);
        if (key != null)
        {
            // delete from datastore
            try
            {
                if (deleteEntry(ctx, key))
                {
                    ctx.getLogger().info("Deleted resource {}, key={}", id, key);
                    return ctx.sendSuccess(204);
                }
            }
            catch (Exception e)
            {
                return ctx.sendError(500, e.getMessage());
            }
        }
        
        return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
    }
    
    
    protected boolean deleteEntry(final ResourceContext ctx, final K key) throws DataStoreException
    {        
        return dataStore.remove(key) != null;
    }
    
    
    protected boolean getElementCount(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        F filter = getFilter(ctx.getParentRef(), ctx.req.getParameterMap());
        
        // get count from datastore
        long count = dataStore.countMatchingEntries(filter);
        //long count = dataStore.getNumRecords();
        
        // write out as json
        Writer writer = new OutputStreamWriter(ctx.resp.getOutputStream());
        writer.write("{\"count\":");
        writer.write(Long.toString(count));
        writer.write('}');
        writer.flush();
        
        return true;
    }
    
    
    protected IResourceHandler getSubResource(ResourceContext ctx, String id)
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return null;
        
        // decode internal ID for nested resource
        long internalID = decodeID(ctx, id);
        if (internalID <= 0)
            return null;
        
        // check that resource ID valid
        if (!isValidID(internalID))
        {
            ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
            return null;
        }
                
        ctx.setParent(this, internalID);
        return resource;
    }
    
    
    protected String getCanonicalResourceUrl(final String id, final HttpServletRequest req)
    {
        //return req.getRequestURL().append('/').append(id).toString();
        StringBuilder buf = new StringBuilder();
        buf.append('/').append(getNames()[0]);
        buf.append('/').append(id);
        return buf.toString();
    }
    
    
    protected long decodeID(final ResourceContext ctx, final String id)
    {
        try
        {
            long encodedID = Long.parseLong(id, ResourceBinding.ID_RADIX);
            long decodedID = idEncoder.decodeID(encodedID);
            
            // stop here if hash is invalid
            if (decodedID <= 0)
                ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
            
            return decodedID;
        }
        catch (NumberFormatException e)
        {
            ctx.sendError(400, String.format("Invalid resource identifier: %s", id));
            return 0;
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
                        throw new InvalidRequestException("Invalid select parameter: " + val);
                    
                    else if (item.startsWith("!"))
                        propFilter.excludedProps.add(item.substring(1));
                    else
                        propFilter.includedProps.add(item);
                }
                
                if (propFilter.includedProps.isEmpty() && propFilter.excludedProps.isEmpty())
                    throw new InvalidRequestException("Invalid select parameter: " + val);
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
                        throw new InvalidRequestException("Invalid resource ID: " + id);
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
            throw new InvalidRequestException("Invalid time parameter: " + timeVal);
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
            throw new InvalidRequestException("Invalid bounding box: " + bboxCoords);
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
            throw new InvalidRequestException("Invalid geometry: " + wkt);
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
            throw new InvalidRequestException("Invalid " + paramName + " parameter: " + paramValue);
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
            throw new InvalidRequestException("Too many '" + paramName + "' parameters in query");
        
        if (paramValues.isEmpty())
            return null;
        
        return paramValues.iterator().next();
    }
}
