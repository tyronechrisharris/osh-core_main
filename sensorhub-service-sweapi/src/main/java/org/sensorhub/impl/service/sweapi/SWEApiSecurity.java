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

import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.security.IAuthorizer;
import org.sensorhub.api.security.IPermission;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.impl.module.ModuleSecurity;
import org.sensorhub.impl.security.ItemPermission;
import org.sensorhub.impl.security.PermissionRequest;
import org.sensorhub.impl.service.sweapi.resource.ResourceCollectionPermission;
import org.sensorhub.impl.service.sweapi.resource.ResourcePermission;


public class SWEApiSecurity extends ModuleSecurity
{    
    final IPermission resource_view; // view summary during search
    final IPermission resource_read;
    final IPermission resource_create;
    final IPermission resource_update;
    final IPermission resource_delete;
    
    IAuthorizer authorizer;
    
    
    static class DummyModule extends AbstractModule<ModuleConfig>
    {
        @Override
        public String getLocalID()
        {
            return "0";
        }

        @Override
        public void start() throws SensorHubException
        {            
        }

        @Override
        public void stop() throws SensorHubException
        {
        }
    }
    
    
    public SWEApiSecurity(SWEApiService service, boolean enable)
    {
        super(service, "restapi", enable);
        
        // register permission structure
        resource_view = new ItemPermission(null, "View");
        resource_read = new ItemPermission(null, "Read");
        resource_create = new ItemPermission(null, "Create");
        resource_update = new ItemPermission(null, "Update");
        resource_delete = new ItemPermission(null, "Delete");
    }
    
    
    public void checkPermission(long resourceID, IPermission perm) throws SecurityException
    {
        PermissionRequest req = new PermissionRequest(new ResourcePermission(resourceID));
        req.add(perm);
        
        return;
    }
    
    
    public void checkPermission(long parentID, String collectionName, IPermission perm) throws SecurityException
    {
        PermissionRequest req = new PermissionRequest(new ResourceCollectionPermission(parentID, collectionName));
        req.add(perm);
        
        return;
    }
}
