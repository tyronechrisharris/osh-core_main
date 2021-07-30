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
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;


public class RootHandler extends BaseHandler
{
    
    public RootHandler()
    {
    }


    @Override
    public void doGet(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        resource.doGet(ctx);
    }


    @Override
    public void doPost(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        resource.doPost(ctx);
    }


    @Override
    public void doPut(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        resource.doPut(ctx);
    }


    @Override
    public void doDelete(ResourceContext ctx) throws IOException
    {
        IResourceHandler resource = getSubResource(ctx);
        resource.doDelete(ctx);
    }
    
    
    @Override
    public String[] getNames()
    {
        return new String[0];
    }
}
