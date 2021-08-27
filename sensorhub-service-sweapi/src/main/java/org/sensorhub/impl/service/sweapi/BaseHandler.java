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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.vast.util.Bbox;
import org.vast.util.TimeExtent;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;


public abstract class BaseHandler implements IResourceHandler
{
    public static final String INVALID_URI_ERROR_MSG = "Invalid resource URI";
    
    final Map<String, IResourceHandler> subResources = new HashMap<>();

    
    public BaseHandler()
    {
        super();
    }
    

    @Override
    public void addSubResource(IResourceHandler handler)
    {
        addSubResource(handler, handler.getNames());
    }
    

    @Override
    public void addSubResource(IResourceHandler handler, String... names)
    {
        for (var name: names)
            subResources.put(name, handler);
    }
    

    protected IResourceHandler getSubResource(RequestContext ctx) throws InvalidRequestException
    {
        if (ctx == null || ctx.isEndOfPath())
            throw ServiceErrors.badRequest("Missing resource name");
        
        String resourceName = ctx.popNextPathElt();
        IResourceHandler resource = subResources.get(resourceName);
        if (resource == null)
            throw ServiceErrors.badRequest("Invalid resource name: '" + resourceName + "'");
        
        return resource;
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
                        propFilter.getExcludedProps().add(item.substring(1));
                    else
                        propFilter.getIncludedProps().add(item);
                }
                
                if (propFilter.getIncludedProps().isEmpty() && propFilter.getExcludedProps().isEmpty())
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
    
    
    protected Collection<ResourceLink> getPagingLinks(final RequestContext ctx, long offset, long limit, boolean hasMore) throws InvalidRequestException
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

}