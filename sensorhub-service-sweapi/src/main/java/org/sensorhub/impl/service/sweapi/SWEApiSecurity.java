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
import org.sensorhub.impl.service.sweapi.RestApiServlet.ResourcePermissions;


public class SWEApiSecurity extends ModuleSecurity
{
    private static final String NAME_PROCEDURE_SUMMARY = "proc_summary";
    private static final String NAME_PROCEDURE_DETAILS = "proc_details";
    private static final String NAME_SYSTEM_SUMMARY = "system_summary";
    private static final String NAME_SYSTEM_DETAILS = "system_details";
    private static final String NAME_FOI = "fois";
    private static final String NAME_DATASTREAM = "datastreams";
    private static final String NAME_OBS = "obs";
    private static final String NAME_CONTROLS = "controls";
    private static final String NAME_COMMANDS = "commands";
    
    private static final String LABEL_PROCEDURE_SUMMARY = "Procedure Summaries";
    private static final String LABEL_PROCEDURE_DETAILS = "Procedure Details";
    private static final String LABEL_SYSTEM_SUMMARY = "System Summaries";
    private static final String LABEL_SYSTEM_DETAILS = "System Details";
    private static final String LABEL_FOI = "Features of Interest";
    private static final String LABEL_DATASTREAM = "Datastreams Info";
    private static final String LABEL_OBS = "Observations";
    private static final String LABEL_CONTROLS = "Control Channels Info";
    private static final String LABEL_COMMANDS = "Commands";
    
    public final IPermission api_read;
    public final IPermission api_create;
    public final IPermission api_update;
    public final IPermission api_delete;
    public final IPermission api_stream;
    
    public final ResourcePermissions proc_summary_permissions = new ResourcePermissions();
    public final ResourcePermissions proc_details_permissions = new ResourcePermissions();
    public final ResourcePermissions system_summary_permissions = new ResourcePermissions();
    public final ResourcePermissions system_details_permissions = new ResourcePermissions();
    public final ResourcePermissions foi_permissions = new ResourcePermissions();
    public final ResourcePermissions datastream_permissions = new ResourcePermissions();
    public final ResourcePermissions obs_permissions = new ResourcePermissions();
    public final ResourcePermissions commandstream_permissions = new ResourcePermissions();
    public final ResourcePermissions command_permissions = new ResourcePermissions();
    
    IAuthorizer authorizer;
    
    
    static class DummyModule extends AbstractModule<ModuleConfig>
    {
        @Override
        public String getLocalID()
        {
            return "0";
        }

        @Override
        protected void doStart() throws SensorHubException
        {            
        }

        @Override
        protected void doStop() throws SensorHubException
        {
        }
    }
    
    
    public SWEApiSecurity(SWEApiService service, boolean enable)
    {
        super(service, "swapi", enable);
        
        // register permission structure
        api_read = new ItemPermission(rootPerm, "get");
        proc_summary_permissions.read = new ItemPermission(api_read, NAME_PROCEDURE_SUMMARY, LABEL_PROCEDURE_SUMMARY);
        proc_details_permissions.read = new ItemPermission(api_read, NAME_PROCEDURE_DETAILS, LABEL_PROCEDURE_DETAILS);
        system_summary_permissions.read = new ItemPermission(api_read, NAME_SYSTEM_SUMMARY, LABEL_SYSTEM_SUMMARY);
        system_details_permissions.read = new ItemPermission(api_read, NAME_SYSTEM_DETAILS, LABEL_SYSTEM_DETAILS);
        foi_permissions.read = new ItemPermission(api_read, NAME_FOI, LABEL_FOI);
        datastream_permissions.read = new ItemPermission(api_read, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.read = new ItemPermission(api_read, NAME_OBS, LABEL_OBS);
        commandstream_permissions.read = new ItemPermission(api_read, NAME_CONTROLS, LABEL_CONTROLS);
        command_permissions.read = new ItemPermission(api_read, NAME_COMMANDS, LABEL_COMMANDS);
        
        api_create = new ItemPermission(rootPerm, "create");
        proc_summary_permissions.create = new ItemPermission(api_create, NAME_PROCEDURE_SUMMARY, LABEL_PROCEDURE_SUMMARY);
        proc_details_permissions.create = new ItemPermission(api_create, NAME_PROCEDURE_DETAILS, LABEL_PROCEDURE_DETAILS);
        system_summary_permissions.create = new ItemPermission(api_create, NAME_SYSTEM_SUMMARY, LABEL_SYSTEM_SUMMARY);
        system_details_permissions.create = new ItemPermission(api_create, NAME_SYSTEM_DETAILS, LABEL_SYSTEM_DETAILS);
        foi_permissions.create = new ItemPermission(api_create, NAME_FOI, LABEL_FOI);
        datastream_permissions.create = new ItemPermission(api_create, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.create = new ItemPermission(api_create, NAME_OBS, LABEL_OBS);
        commandstream_permissions.create = new ItemPermission(api_create, NAME_CONTROLS, LABEL_CONTROLS);
        command_permissions.create = new ItemPermission(api_create, NAME_COMMANDS, LABEL_COMMANDS);
        
        api_update = new ItemPermission(rootPerm, "update");
        proc_summary_permissions.update = new ItemPermission(api_update, NAME_PROCEDURE_SUMMARY, LABEL_PROCEDURE_SUMMARY);
        proc_details_permissions.update = new ItemPermission(api_update, NAME_PROCEDURE_DETAILS, LABEL_PROCEDURE_DETAILS);
        system_summary_permissions.update = new ItemPermission(api_update, NAME_SYSTEM_SUMMARY, LABEL_SYSTEM_SUMMARY);
        system_details_permissions.update = new ItemPermission(api_update, NAME_SYSTEM_DETAILS, LABEL_SYSTEM_DETAILS);
        foi_permissions.update = new ItemPermission(api_update, NAME_FOI, LABEL_FOI);
        datastream_permissions.update = new ItemPermission(api_update, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.update = new ItemPermission(api_update, NAME_OBS, LABEL_OBS);
        commandstream_permissions.update = new ItemPermission(api_update, NAME_CONTROLS, LABEL_CONTROLS);
        command_permissions.update = new ItemPermission(api_update, NAME_COMMANDS, LABEL_COMMANDS);
        
        api_delete = new ItemPermission(rootPerm, "delete");
        proc_summary_permissions.delete = new ItemPermission(api_delete, NAME_PROCEDURE_SUMMARY, LABEL_PROCEDURE_SUMMARY);
        proc_details_permissions.delete = new ItemPermission(api_delete, NAME_PROCEDURE_DETAILS, LABEL_PROCEDURE_DETAILS);
        system_summary_permissions.delete = new ItemPermission(api_delete, NAME_SYSTEM_SUMMARY, LABEL_SYSTEM_SUMMARY);
        system_details_permissions.delete = new ItemPermission(api_delete, NAME_SYSTEM_DETAILS, LABEL_SYSTEM_DETAILS);
        foi_permissions.delete = new ItemPermission(api_delete, NAME_FOI, LABEL_FOI);
        datastream_permissions.delete = new ItemPermission(api_delete, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.delete = new ItemPermission(api_delete, NAME_OBS, LABEL_OBS);
        commandstream_permissions.delete = new ItemPermission(api_delete, NAME_CONTROLS, LABEL_CONTROLS);
        command_permissions.delete = new ItemPermission(api_delete, NAME_COMMANDS, LABEL_COMMANDS);
        
        api_stream = new ItemPermission(rootPerm, "stream");
        proc_summary_permissions.stream = new ItemPermission(api_stream, NAME_PROCEDURE_SUMMARY, LABEL_PROCEDURE_SUMMARY);
        proc_details_permissions.stream = new ItemPermission(api_stream, NAME_PROCEDURE_DETAILS, LABEL_PROCEDURE_DETAILS);
        system_summary_permissions.stream = new ItemPermission(api_stream, NAME_SYSTEM_SUMMARY, LABEL_SYSTEM_SUMMARY);
        system_details_permissions.stream = new ItemPermission(api_stream, NAME_SYSTEM_DETAILS, LABEL_SYSTEM_DETAILS);
        foi_permissions.stream = new ItemPermission(api_stream, NAME_FOI, LABEL_FOI);
        datastream_permissions.stream = new ItemPermission(api_stream, NAME_DATASTREAM, LABEL_DATASTREAM);
        obs_permissions.stream = new ItemPermission(api_stream, NAME_OBS, LABEL_OBS);
        commandstream_permissions.stream = new ItemPermission(api_stream, NAME_CONTROLS, LABEL_CONTROLS);
        command_permissions.stream = new ItemPermission(api_stream, NAME_COMMANDS, LABEL_COMMANDS);
        
        
        // register wildcard permission tree usable for all SOS services
        // do it at this point so we don't include specific offering permissions
        ModulePermissions wildcardPerm = rootPerm.cloneAsTemplatePermission("SensorWeb API Services");
        service.getParentHub().getSecurityManager().registerModulePermissions(wildcardPerm);
        
        // register permission tree
        service.getParentHub().getSecurityManager().registerModulePermissions(rootPerm);
    }
}
