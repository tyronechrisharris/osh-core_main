/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import org.sensorhub.api.security.IPermission;
import org.sensorhub.impl.module.ModuleSecurity;
import org.slf4j.Logger;


@SuppressWarnings("serial")
public abstract class RestApiServlet extends HttpServlet
{
    
    public static class ResourcePermissions
    {
        public IPermission read;
        public IPermission create;
        public IPermission update;
        public IPermission delete;
        public IPermission stream;
        
        public ResourcePermissions() {}
    }
    
    
    public abstract String getApiRootURL(HttpServletRequest req);
    
    public abstract Logger getLogger();
    
    public abstract ModuleSecurity getSecurityHandler();
    
}
