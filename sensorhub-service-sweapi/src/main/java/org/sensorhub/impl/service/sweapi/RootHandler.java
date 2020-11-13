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


public class RootHandler extends BaseHandler
{
    
    public RootHandler()
    {
    }


    @Override
    public boolean doGet(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return false;
        
        return resource.doGet(ctx);
    }


    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return false;
        
        return resource.doPost(ctx);
    }


    @Override
    public boolean doPut(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return false;
        
        return resource.doPut(ctx);
    }


    @Override
    public boolean doDelete(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        if (resource == null)
            return false;
        
        return resource.doDelete(ctx);
    }
    
    
    @Override
    public String[] getNames()
    {
        return new String[0];
    }
}
