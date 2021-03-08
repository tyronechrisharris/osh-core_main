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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.websocket.api.WebSocketBehavior;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.security.ISecurityManager;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.slf4j.Logger;


@SuppressWarnings("serial")
public class SWEApiServlet extends HttpServlet
{
    static final String LOG_REQUEST_MSG = "{} {}{} (from ip={}, user={})";
    static final String INTERNAL_ERROR_MSG = "Internal server error";
    static final String INTERNAL_ERROR_LOG_MSG = INTERNAL_ERROR_MSG + " while processing request " + LOG_REQUEST_MSG;
    static final String JSON_CONTENT_TYPE = "application/json";

    protected final SWEApiServiceConfig config;
    protected final SWEApiSecurity securityHandler;
    protected IProcedureStore procedures;
    protected IFeatureStore features;
    protected IResourceHandler rootHandler;
    protected Executor threadPool;
    protected WebSocketServletFactory wsFactory;
    protected Logger log;
    

    public SWEApiServlet(SWEApiService service, SWEApiSecurity securityHandler, RootHandler rootHandler, Logger logger)
    {
        this.config = service.getConfiguration();
        this.threadPool = service.getThreadPool();
        this.securityHandler = securityHandler;
        this.rootHandler = rootHandler;
        this.log = logger;        
    }


    @Override
    public void init(ServletConfig config) throws ServletException
    {
        super.init(config);

        // create websocket factory
        try
        {
            WebSocketPolicy wsPolicy = new WebSocketPolicy(WebSocketBehavior.SERVER);
            wsFactory = WebSocketServletFactory.Loader.load(getServletContext(), wsPolicy);
            wsFactory.start();
        }
        catch (Exception e)
        {
            throw new ServletException("Cannot initialize websocket factory", e);
        }
    }


    @Override
    public void destroy()
    {
        // destroy websocket factory
        try
        {
            wsFactory.stop();
        }
        catch (Exception e)
        {
            log.error("Cannot stop websocket factory", e);
        }
    }
    
    
    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        logRequest(req);
        super.service(req, resp);
    }
    
    
    protected void setCurrentUser(HttpServletRequest req)
    {
        String userID = ISecurityManager.ANONYMOUS_USER;
        if (req.getRemoteUser() != null)
            userID = req.getRemoteUser();
        securityHandler.setCurrentUser(userID);
    }
    
    
    protected void clearCurrentUser()
    {
        securityHandler.clearCurrentUser();
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        resp.setContentType(JSON_CONTENT_TYPE);
        
        // parse resource path
        final ResourceContext ctx = createContext(req, resp);
        if (ctx == null)
        {
            resp.setStatus(400);
            return;
        }
        
        // handle request asynchronously
        try
        {
            final AsyncContext aCtx = req.startAsync(req, resp);
            CompletableFuture.runAsync(() -> {
                try
                {
                    setCurrentUser(req);
                    rootHandler.doGet(ctx);
                }
                catch (Exception e)
                {
                    logError(req, e);
                    sendError(500, INTERNAL_ERROR_MSG, resp);
                }
                finally
                {                
                    clearCurrentUser();
                    aCtx.complete();
                }
            }, threadPool);
        }
        catch (Exception e)
        {
            logError(req, e);
            sendError(500, INTERNAL_ERROR_MSG, resp);
        }
    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        resp.setContentType(JSON_CONTENT_TYPE);
        
        // parse resource path
        final ResourceContext ctx = createContext(req, resp);
        if (ctx == null)
        {
            resp.setStatus(400);
            return;
        }
        
        // handle request asynchronously
        try
        {
            final AsyncContext aCtx = req.startAsync(req, resp);
            CompletableFuture.runAsync(() -> {
                try
                {
                    setCurrentUser(req);
                    rootHandler.doPost(ctx);
                }
                catch (Throwable e)
                {
                    logError(req, e);
                    sendError(500, INTERNAL_ERROR_MSG, resp);
                }
                finally
                {                
                    clearCurrentUser();
                    aCtx.complete();
                }
            }, threadPool);
        }
        catch (Exception e)
        {
            logError(req, e);
            sendError(500, INTERNAL_ERROR_MSG, resp);
        }
    }


    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        resp.setContentType(JSON_CONTENT_TYPE);
        
        // parse resource path
        final ResourceContext ctx = createContext(req, resp);
        if (ctx == null)
        {
            sendError(400, resp);
            return;
        }
        
        // handle request asynchronously
        try
        {
            final AsyncContext aCtx = req.startAsync(req, resp);
            CompletableFuture.runAsync(() -> {
                try
                {
                    setCurrentUser(req);
                    rootHandler.doPut(ctx);
                }
                catch (Throwable e)
                {
                    logError(req, e);
                    sendError(500, INTERNAL_ERROR_MSG, resp);
                }
                finally
                {                
                    clearCurrentUser();
                    aCtx.complete();
                }
            }, threadPool);
        }
        catch (Exception e)
        {
            logError(req, e);
            sendError(500, INTERNAL_ERROR_MSG, resp);
        }
    }


    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        resp.setContentType(JSON_CONTENT_TYPE);
        
        // parse resource path
        final ResourceContext ctx = createContext(req, resp);
        if (ctx == null)
        {
            sendError(400, resp);
            return;
        }
        
        // handle request asynchronously
        try
        {
            final AsyncContext aCtx = req.startAsync(req, resp);
            CompletableFuture.runAsync(() -> {
                try
                {
                    setCurrentUser(req);
                    rootHandler.doDelete(ctx);
                }
                catch (Throwable e)
                {
                    logError(req, e);
                    sendError(500, INTERNAL_ERROR_MSG, resp);
                }
                finally
                {                
                    clearCurrentUser();
                    aCtx.complete();
                }
            }, threadPool);
        }
        catch (Exception e)
        {
            logError(req, e);
            sendError(500, INTERNAL_ERROR_MSG, resp);
        }
    }
    
    
    protected ResourceContext createContext(HttpServletRequest req, HttpServletResponse resp)
    {
        // check if we have an upgrade request for websockets
        if (wsFactory.isUpgradeRequest(req, resp))
            return new ResourceContext(this, req, resp, wsFactory);
        else
            return new ResourceContext(this, req, resp);
        
    }
    
    
    protected void logRequest(HttpServletRequest req)
    {
        if (log.isInfoEnabled())
            logRequestInfo(req, null);
    }
    
    
    protected void logError(HttpServletRequest req, Throwable e)
    {
        if (log.isErrorEnabled())
            logRequestInfo(req, e);
    }
    
    
    protected void logRequestInfo(HttpServletRequest req, Throwable error)
    {
        String method = req.getMethod();
        String url = req.getRequestURI();
        String ip = req.getRemoteAddr();
        String user = req.getRemoteUser() != null ? req.getRemoteUser() : "anonymous";
        
        // if proxy header present, use source ip instead of proxy ip
        String proxyHeader = req.getHeader("X-Forwarded-For");
        if (proxyHeader != null)
        {
            String[] ips = proxyHeader.split(",");
            if (ips.length >= 1)
                ip = ips[0];
        }
        
        // detect websocket upgrade
        if ("websocket".equalsIgnoreCase(req.getHeader("Upgrade")))
            method += "/Websocket";
        
        // append decoded request if any
        String query = "";
        if (req.getQueryString() != null)
            query = "?" + req.getQueryString();
        
        if (error != null)
            log.error(INTERNAL_ERROR_LOG_MSG, method, url, query, ip, user, error);
        else
            log.info(LOG_REQUEST_MSG, method, url, query, ip, user);
    }
    
    
    public void sendError(int code, HttpServletResponse resp)
    {
        sendError(code, null, resp);
    }
    
    
    public void sendError(int code, String msg, HttpServletResponse resp)
    {
        try
        {
            resp.setStatus(code);
            
            if (msg != null)
            {
                msg = "{ \"error\": \"" + msg + "\" }";
                resp.getOutputStream().write(msg.getBytes());
            }
        }
        catch (IOException e)
        {
            log.error("Could not send error response", e);
        }
    }
    
    
    public Logger getLogger()
    {
        return this.log;
    }


    public SWEApiServiceConfig getConfig()
    {
        return config;
    }


    public SWEApiSecurity getSecurityHandler()
    {
        return securityHandler;
    }

}
