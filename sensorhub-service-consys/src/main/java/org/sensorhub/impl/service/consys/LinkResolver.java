/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2023 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys;

import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.impl.service.consys.procedure.ProcedureHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.system.SystemHandler;
import net.opengis.OgcProperty;


public class LinkResolver
{

    public static void resolveProcedureLink(final RequestContext ctx, final OgcProperty<?> link, final IProcedureDatabase db, final IdEncoders idEncoders)
    {
        synchronized(link)
        {
            // resolve URN to URL
            if (link != null && link.hasHref() && link.getHref().startsWith("urn")
                && db.getProcedureStore() != null)
            {
                var urn = link.getHref();
                var procKey = db.getProcedureStore().getCurrentVersionKey(urn);
                if (procKey != null)
                {
                    link.setRole(urn);
                    var procId = idEncoders.getProcedureIdEncoder().encodeID(procKey.getInternalID());
                    link.setHref(ctx.getApiRootURL() + "/" + ProcedureHandler.NAMES[0] + "/" + procId);
                }
            }
        }
    }
    
    
    public static void resolveSystemLink(final RequestContext ctx, final OgcProperty<?> link, final IObsSystemDatabase db, final IdEncoders idEncoders)
    {
        synchronized(link)
        {
            // resolve URN to URL
            if (link != null && link.hasHref() && link.getHref().startsWith("urn")
                && db.getSystemDescStore() != null)
            {
                var urn = link.getHref();
                var sysKey = db.getSystemDescStore().getCurrentVersionKey(urn);
                if (sysKey != null)
                {
                    link.setRole(urn);
                    var procId = idEncoders.getSystemIdEncoder().encodeID(sysKey.getInternalID());
                    link.setHref(ctx.getApiRootURL() + "/" + SystemHandler.NAMES[0] + "/" + procId);
                }
            }
        }
    }
}
