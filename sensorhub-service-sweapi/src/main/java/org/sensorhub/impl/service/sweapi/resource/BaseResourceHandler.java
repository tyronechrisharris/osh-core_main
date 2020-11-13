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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.sensorhub.api.datastore.IDataStore;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.impl.service.sweapi.BaseHandler;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.StreamException;
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
    public static final String INVALID_JSON_ERROR_MSG = "Invalid JSON: ";
    
    protected final S dataStore;
    protected final ResourceType<K, V> resourceType;
    
    
    public BaseResourceHandler(S dataStore, ResourceType<K, V> resourceType)
    {
        this.dataStore = Asserts.checkNotNull(dataStore, IDataStore.class);
        this.resourceType = Asserts.checkNotNull(resourceType, ResourceType.class);
    }
    
    protected abstract K getKey(final ResourceContext ctx, final String id);
    protected abstract F getFilter(final ResourceRef parent, final Map<String, String[]> queryParams) throws InvalidRequestException;
    protected abstract String addToDataStore(ResourceContext ctx, V resource);
    protected abstract void validate(V resource);
    
    
    @Override
    public boolean doGet(final ResourceContext ctx) throws IOException
    {
        try
        {
            if (ctx.isEmpty())
                return list(ctx);
         
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
        catch (SecurityException e)
        {
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
        }
    }
    
    
    protected boolean getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // get resource key
        var key = getKey(ctx, id);
        if (key == null)
            return false;
        
        // fetch from data store
        final V res = dataStore.get(key);
        if (res == null)
            return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
        
        var queryParams = ctx.req.getParameterMap();
        ctx.resp.setStatus(200);
        resourceType.serialize(key, res, parseSelectArg(queryParams), ResourceFormat.GEOJSON, ctx.resp.getOutputStream());
        return true;
    }
    
    
    protected boolean create(final ResourceContext ctx) throws IOException
    {
        int count = 0;
        String url = null;
        
        // detect array
        InputStream is = new BufferedInputStream(ctx.req.getInputStream());
        is.mark(1);
        boolean isArray = is.read() == '[';
        is.reset();
        
        // read single resource or resource array
        Iterator<? extends V> resources;
        try
        {
            if (isArray)
                resources = resourceType.deserializeArray(ResourceFormat.GEOJSON, is);
            else
                resources = Arrays.asList(resourceType.deserialize(null, is)).iterator();
            
            if (resources == null || !resources.hasNext())
                return ctx.sendError(400, "No data provided");
        }
        catch (JsonParseException | EOFException e)
        {
            String msg = INVALID_JSON_ERROR_MSG + e.getMessage();
            return ctx.sendError(400, msg);
        }
        
        // prepare to write JSON response
        JsonWriter writer = new JsonWriter(new OutputStreamWriter(ctx.resp.getOutputStream()));
        writer.beginArray();
        
        while (resources.hasNext())
        {
            count++;
            
            V res;
            try
            {
                res = resources.next();
            }
            catch (JsonParseException | StreamException e)
            {
                String msg = INVALID_JSON_ERROR_MSG + e.getMessage();
                ctx.writeError(msg, writer);
                continue;
            }
            
            try
            {
                validate(res);
            }
            catch (Exception e)
            {
                String msg = INVALID_JSON_ERROR_MSG + e.getMessage();
                ctx.writeError(msg, writer);
                continue;
            }
            
            // add resource to datastore
            try
            {
                url = addToDataStore(ctx, res);
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
        
        if (url == null)
            ctx.resp.setStatus(400);            
        else if (count == 1)
            ctx.resp.setHeader("Location", url);
        
        writer.endArray();
        writer.flush();
        
        return (url != null);
    }
    
    
    protected boolean update(final ResourceContext ctx, final String id) throws IOException
    {
        // get resource key
        var key = getKey(ctx, id);
        if (key == null)
            return false;
        
        V res;
        try
        {
            res = resourceType.deserialize(ResourceFormat.GEOJSON, ctx.req.getInputStream());
            if (res == null)
                return ctx.sendError(400, "No data provided");
            validate(res);
        }
        catch (JsonParseException e)
        {
            return ctx.sendError(400, INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        // TODO implement parent ID
        // add parent ID to resource object if obtained from URL
        //if (res.getParentID() <= 0 && parent.internalID > 0)
        //    ((Resource)res).setParent(parent.type.getTypeCode(), parent.internalID);
        
        // update resource
        try
        {
            //int version = (res instanceof VersionedResource) ? ((VersionedResource)res).getVersion() : 1;
            res = dataStore.replace(key, res);
            if (res != null)
            {
                ctx.getLogger().info("Updated resource {}, key={}", id, key);
                return ctx.sendSuccess(204);
            }
            else
                return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
        }
        catch (Exception e)
        {
            return ctx.sendError(400, e.getMessage());
        }
                
    }
    
    
    protected boolean delete(final ResourceContext ctx, final String id) throws IOException
    {
        // get resource key
        var key = getKey(ctx, id);
        if (key == null)
            return false;
        
        // delete resource
        V res = dataStore.remove(key);
        if (res != null)
        {
            ctx.getLogger().info("Deleted resource {}, key={}", id, key);
            return ctx.sendSuccess(204);
        }
        else
            return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
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
    
    
    protected boolean list(final ResourceContext ctx) throws InvalidRequestException, IOException
    {
        var queryParams = ctx.req.getParameterMap();
        F filter = getFilter(ctx.getParentRef(), queryParams);
        
        // stream and serialize all resources to servlet output
        var results = dataStore.selectEntries(filter);
        resourceType.serialize(results.sequential(), null, parseSelectArg(queryParams), ResourceFormat.GEOJSON, ctx.resp.getOutputStream());
        
        return true;
    }
    
    
    protected IResourceHandler getSubResource(ResourceContext ctx, String id)
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return null;
        
        // decode internal ID for nested resource
        long internalID = getInternalID(ctx, id);
        if (internalID <= 0)
            return null;
        
        ctx.setParent(resourceType, internalID);
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
    
    
    protected long getInternalID(final ResourceContext ctx, final String id)
    {
        try
        {
            long externalID = Long.parseLong(id, ResourceType.ID_RADIX);
            long internalID = resourceType.getInternalID(externalID);
            
            // stop here if hash is invalid
            if (internalID <= 0)
                ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
            
            return internalID;
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
    
    
    protected Collection<String> parseMultiValuesArg(String[] paramValues)
    {
        var allValues = new ArrayList<String>();
        
        for (String val: paramValues)
        {
            for (String item: val.split(","))
                allValues.add(item);
        }
        
        return allValues;
    }
    
    
    protected Collection<Long> parseResourceIds(String[] paramValues)
    {
        var allValues = new ArrayList<Long>();
        
        for (String val: paramValues)
        {
            for (String id: val.split(","))
            {
                long externalID = Long.parseLong(id, ResourceType.ID_RADIX);
                long internalID = resourceType.getInternalID(externalID);
                allValues.add(internalID);
            }
        }
        
        return allValues;
    }
    
    
    protected TimeExtent parseTimeStampArg(String[] paramValues) throws InvalidRequestException
    {
        var timeVal = getSingleParam("time", paramValues);
        
        try
        {
            return TimeExtent.parse(timeVal);
        }
        catch (Exception e)
        {
            throw new InvalidRequestException("Invalid time parameter: " + timeVal);
        }
    }
    
    
    protected Bbox parseBboxArg(String[] paramValues) throws InvalidRequestException
    {
        var bboxTxt = getSingleParam("bbox", paramValues);
        
        try
        {
            String[] coords = bboxTxt.split(",");
            Bbox bbox = new Bbox();
            bbox.setMinX(Double.parseDouble(coords[0]));
            bbox.setMinY(Double.parseDouble(coords[1]));
            bbox.setMaxX(Double.parseDouble(coords[2]));
            bbox.setMaxY(Double.parseDouble(coords[3]));
            bbox.checkValid();
            return bbox;
        }
        catch (Exception e)
        {
            throw new InvalidRequestException("Invalid bounding box: " + bboxTxt);
        }
    }
    
    
    protected Geometry parseGeomArg(String[] paramValues) throws InvalidRequestException
    {
        var wkt = getSingleParam("geom", paramValues);
        
        try
        {
            return new WKTReader().read(wkt);
        }
        catch (ParseException e)
        {
            throw new InvalidRequestException("Invalid geometry: " + wkt);
        }
    }
    
    
    protected String getSingleParam(String paramName, String[] paramValues) throws InvalidRequestException
    {
        if (paramValues.length > 1)
            throw new InvalidRequestException("Too many '" + paramName + "' parameters in query");
        
        return paramValues[0];
    }
}
