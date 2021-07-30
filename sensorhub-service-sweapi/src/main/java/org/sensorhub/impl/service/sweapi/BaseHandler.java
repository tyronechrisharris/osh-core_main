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
    

    protected IResourceHandler getSubResource(ResourceContext ctx) throws InvalidRequestException
    {
        if (ctx == null || ctx.isEndOfPath())
            throw ServiceErrors.badRequest("Missing resource name");
        
        String resourceName = ctx.popNextPathElt();
        IResourceHandler resource = subResources.get(resourceName);
        if (resource == null)
            throw ServiceErrors.badRequest("Invalid resource name: '" + resourceName + "'");
        
        return resource;
    }

}