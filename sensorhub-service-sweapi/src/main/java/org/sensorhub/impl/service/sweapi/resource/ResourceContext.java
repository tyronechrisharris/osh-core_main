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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity;
import org.sensorhub.impl.service.sweapi.SWEApiServlet;
import org.sensorhub.impl.service.sweapi.stream.StreamHandler;
import org.slf4j.Logger;
import org.vast.util.Asserts;
import com.google.common.base.Strings;


public class ResourceContext
{
    private final SWEApiServlet servlet;
    private final HttpServletRequest req;
    private final HttpServletResponse resp;
    private final StreamHandler streamHandler;
    Deque<String> path;
    Map<String, String[]> queryParams;
    ResourceRef parentResource = new ResourceRef();
    ResourceFormat format;
    PropertyFilter propFilter;
    Set<String> resourceUris;
    
    
    /*
     * Auxiliary data generated during the request handling process for
     * consumption by later processing stages
     */
    Object data;
    
    
    public static class ResourceRef
    {
        @SuppressWarnings("rawtypes")
        public BaseResourceHandler type;
        public long internalID;
        public long version;
    }
    
    
    public ResourceContext(SWEApiServlet servlet, HttpServletRequest req, HttpServletResponse resp)
    {
        this(servlet, req, resp, null);
    }
    
    
    public ResourceContext(SWEApiServlet servlet, HttpServletRequest req, HttpServletResponse resp, StreamHandler streamHandler)
    {
        this.servlet = Asserts.checkNotNull(servlet, Servlet.class);
        this.req = Asserts.checkNotNull(req, HttpServletRequest.class);
        this.resp = Asserts.checkNotNull(resp, HttpServletResponse.class);
        this.streamHandler = streamHandler;
        this.path = splitPath(req.getPathInfo());
        this.queryParams = req.getParameterMap();
    }
    
    
    public ResourceContext(SWEApiServlet servlet, String path, Map<String, String[]> queryParams, StreamHandler streamHandler)
    {
        this.servlet = Asserts.checkNotNull(servlet, Servlet.class);
        this.req = null;
        this.resp = null;
        this.streamHandler = Asserts.checkNotNull(streamHandler, StreamHandler.class);
        this.path = splitPath(path);
        this.queryParams = Asserts.checkNotNull(queryParams, "queryParams");
    }
    
    
    private Deque<String> splitPath(String pathStr)
    {
        if (pathStr != null)
        {
            String[] pathElts = pathStr.split("/");
            var path = new ArrayDeque<String>(pathElts.length);
            for (String elt: pathElts)
            {
                if (!Strings.isNullOrEmpty(elt))
                    path.addLast(elt);
            }
            
            return path;
        }
        
        return new ArrayDeque<String>(0);
    }
    
    
    public boolean isEndOfPath()
    {
        return path.isEmpty();
    }
    
    
    public String popNextPathElt()
    {
        if (path.isEmpty())
            return null;
        return path.pollFirst();
    }
    
    
    public long getParentID()
    {
        return parentResource.internalID;
    }
    
    
    public ResourceRef getParentRef()
    {
        return parentResource;
    }
    
    
    public void setParent(@SuppressWarnings("rawtypes") BaseResourceHandler parentHandler, long internalID)
    {
        parentResource.type = parentHandler;
        parentResource.internalID = internalID;
    }
    
    
    public void setParent(@SuppressWarnings("rawtypes") BaseResourceHandler parentHandler, long internalID, long version)
    {
        parentResource.type = parentHandler;
        parentResource.internalID = internalID;
        parentResource.version = internalID;
    }
    
    
    public void setFormatOptions(ResourceFormat format, PropertyFilter propFilter)
    {
        this.format = Asserts.checkNotNull(format, ResourceFormat.class);
        this.propFilter = propFilter;
    }
    
    
    public ResourceFormat getFormat()
    {
        return format;
    }


    public PropertyFilter getPropertyFilter()
    {
        return propFilter;
    }


    public Object getData()
    {
        return data;
    }
    
    
    public void setData(Object data)
    {
        this.data = data;
    }
    
    
    public String getRequestContentType()
    {
        return req != null ? req.getContentType() : null;            
    }
    
    
    public void setResponseContentType(String mimeType)
    {
        if (resp != null)
            resp.setContentType(mimeType);
    }
   
    
    
    /*public boolean sendError(int code)
    {
        return sendError(code, null);
    }
    
    
    public boolean sendError(int code, String msg)
    {
        if (resp != null)
            servlet.sendError(code, msg, resp);
        return false;
    }
    
    
    public boolean sendSuccess(int code)
    {
        return sendSuccess(code, null);
    }
    
    
    public boolean sendSuccess(int code, String msg)
    {
        try
        {
            resp.setStatus(code);
            if (msg != null)
                resp.getOutputStream().write(msg.getBytes());
        }
        catch (IOException e)
        {
            getLogger().error("Could not send response", e);
        }
        
        return true;
    }
    
    
    protected void writeError(String msg, JsonWriter writer) throws IOException
    {
        writer.beginObject();
        writer.name("error").value(msg);
        writer.endObject();
    }*/
    
    
    public Logger getLogger()
    {
        return servlet.getLogger();
    }
    
    
    public String getApiRootURL()
    {
        return servlet.getApiRootURL();
    }
    
    
    public Map<String, String[]> getParameterMap()
    {
        return queryParams;
    }
    
    
    public String getParameter(String name)
    {
        var paramValues = queryParams.get(name);
        if (paramValues != null && paramValues.length > 0)
            return paramValues[0];
        else
            return null;
    }
    
    
    public StreamHandler getStreamHandler()
    {
        return streamHandler;
    }
    
    
    public InputStream getInputStream() throws IOException
    {
        /*return streamHandler != null ?
            streamHandler.getInputStream() :
            req.getInputStream();*/
        return req != null ? req.getInputStream() : null;
    }
    
    public OutputStream getOutputStream() throws IOException
    {
        return streamHandler != null ?
            streamHandler.getOutputStream() :
            resp.getOutputStream();
    }
    
    
    public boolean isStreamRequest()
    {
        return streamHandler != null;
    }
    
    
    public SWEApiSecurity getSecurityHandler()
    {
        return servlet.getSecurityHandler();
    }


    public Set<String> getResourceUris()
    {
        return resourceUris != null ? resourceUris : Collections.emptySet();
    }


    public void addResourceUri(String uri)
    {
        if (resourceUris == null)
            resourceUris = new LinkedHashSet<>();
        resourceUris.add(uri);
    }
}
