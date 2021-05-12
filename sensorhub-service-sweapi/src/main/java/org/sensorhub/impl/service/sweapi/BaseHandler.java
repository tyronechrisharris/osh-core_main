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

import java.util.HashMap;
import java.util.Map;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;


public abstract class BaseHandler implements IResourceHandler
{
    public static final String ACCESS_DENIED_ERROR_MSG = "Permission denied";
    
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
    

    protected IResourceHandler getSubResource(ResourceContext ctx)
    {
        if (ctx == null || ctx.isEmpty())
        {
            ctx.sendError(400, "Missing resource name");
            return null;
        }        
        
        String resourceName = ctx.popNextPathElt();
        IResourceHandler resource = subResources.get(resourceName);
        if (resource == null)
        {
            ctx.sendError(404, "Invalid resource name: '" + resourceName + "'");
            return null;
        }
        
        return resource;
    }
    
    
    protected boolean handleAuthException(final ResourceContext ctx, final SecurityException e)
    {
        if (ctx.getRequest().getRemoteUser() == null)
            return ctx.sendAuthenticateRequest();
        else
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
    }

}