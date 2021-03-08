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
import org.sensorhub.impl.security.ModulePermissions;


public class SWEApiSecurity extends ModuleSecurity
{    
    private static final String NAME_PROC_SUMMARY = "proc_summary";
    private static final String NAME_PROC_DETAILS = "proc_details";
    private static final String NAME_FOI = "fois";
    private static final String NAME_DATASTREAM = "datastreams";
    private static final String NAME_OBS = "obs";
    private static final String NAME_COMMANDSTREAM = "command_streams";
    private static final String NAME_TASK = "tasks";
    
    private static final String LABEL_PROC_SUMMARY = "Procedure Summaries";
    private static final String LABEL_PROC_DETAILS = "Procedure Details";
    private static final String LABEL_FOI = "Features of Interest";
    private static final String LABEL_DATASTREAM = "Datastreams Info";
    private static final String LABEL_OBS = "Observations";
    private static final String LABEL_COMMANDSTREAM = "Command Streams Info";
    private static final String LABEL_TASK = "Tasks";
    
    
    public static class ResourcePermissions
    {
        public IPermission read;
        public IPermission create;
        public IPermission update;
        public IPermission delete;
        public IPermission stream;
        
        protected ResourcePermissions() {}
    }
    
    public final IPermission api_read;
    public final IPermission api_create;
    public final IPermission api_update;
    public final IPermission api_delete;
    public final IPermission api_stream;
    
    public final ResourcePermissions proc_summary_permissions = new ResourcePermissions();
    public final ResourcePermissions proc_details_permissions = new ResourcePermissions();
    public final ResourcePermissions foi_permissions = new ResourcePermissions();
    public final ResourcePermissions datastream_permissions = new ResourcePermissions();
    public final ResourcePermissions obs_permissions = new ResourcePermissions();
    
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
        api_read = new ItemPermission(rootPerm, "Read");
        proc_summary_permissions.read = new ItemPermission(api_read, NAME_PROC_SUMMARY, LABEL_PROC_SUMMARY);
        proc_details_permissions.read = new ItemPermission(api_read, NAME_PROC_DETAILS, LABEL_PROC_DETAILS);
        foi_permissions.read = new ItemPermission(api_read, NAME_FOI, LABEL_FOI);
        datastream_permissions.read = new ItemPermission(api_read, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.read = new ItemPermission(api_read, NAME_OBS, LABEL_OBS);
        
        api_create = new ItemPermission(rootPerm, "Create");
        proc_summary_permissions.create = new ItemPermission(api_create, NAME_PROC_SUMMARY, LABEL_PROC_SUMMARY);
        proc_details_permissions.create = new ItemPermission(api_create, NAME_PROC_DETAILS, LABEL_PROC_DETAILS);
        foi_permissions.create = new ItemPermission(api_create, NAME_FOI, LABEL_FOI);
        datastream_permissions.create = new ItemPermission(api_create, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.create = new ItemPermission(api_create, NAME_OBS, LABEL_OBS);
        
        api_update = new ItemPermission(rootPerm, "Update");
        proc_summary_permissions.update = new ItemPermission(api_update, NAME_PROC_SUMMARY, LABEL_PROC_SUMMARY);
        proc_details_permissions.update = new ItemPermission(api_update, NAME_PROC_DETAILS, LABEL_PROC_DETAILS);
        foi_permissions.update = new ItemPermission(api_update, NAME_FOI, LABEL_FOI);
        datastream_permissions.update = new ItemPermission(api_update, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.update = new ItemPermission(api_update, NAME_OBS, LABEL_OBS);
        
        api_delete = new ItemPermission(rootPerm, "Delete");
        proc_summary_permissions.delete = new ItemPermission(api_delete, NAME_PROC_SUMMARY, LABEL_PROC_SUMMARY);
        proc_details_permissions.delete = new ItemPermission(api_delete, NAME_PROC_DETAILS, LABEL_PROC_DETAILS);
        foi_permissions.delete = new ItemPermission(api_delete, NAME_FOI, LABEL_FOI);
        datastream_permissions.delete = new ItemPermission(api_delete, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.delete = new ItemPermission(api_delete, NAME_OBS, LABEL_OBS);
        
        api_stream = new ItemPermission(rootPerm, "Streaming");
        proc_summary_permissions.stream = new ItemPermission(api_stream, NAME_PROC_SUMMARY, LABEL_PROC_SUMMARY);
        proc_details_permissions.stream = new ItemPermission(api_stream, NAME_PROC_DETAILS, LABEL_PROC_DETAILS);
        foi_permissions.stream = new ItemPermission(api_stream, NAME_FOI, LABEL_FOI);
        datastream_permissions.stream = new ItemPermission(api_stream, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.stream = new ItemPermission(api_stream, NAME_OBS, LABEL_OBS);
        
        
        // register wildcard permission tree usable for all SOS services
        // do it at this point so we don't include specific offering permissions
        ModulePermissions wildcardPerm = rootPerm.cloneAsTemplatePermission("SensorWeb API Services");
        service.getParentHub().getSecurityManager().registerModulePermissions(wildcardPerm);
        
        // register permission tree
        service.getParentHub().getSecurityManager().registerModulePermissions(rootPerm);
    }
}
