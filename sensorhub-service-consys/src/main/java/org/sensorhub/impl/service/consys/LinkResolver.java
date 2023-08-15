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
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.impl.service.consys.procedure.ProcedureHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import net.opengis.gml.v32.Reference;


public class LinkResolver
{

    public static void resolveTypeOf(final RequestContext ctx, final Reference typeOf, final IProcedureDatabase db, final IdEncoders idEncoders)
    {
        synchronized(typeOf)
        {
            // resolve URN to URL
            if (typeOf != null && typeOf.hasHref() && typeOf.getHref().startsWith("urn")
                && db.getProcedureStore() != null)
            {
                var urn = typeOf.getHref();
                var procKey = db.getProcedureStore().getCurrentVersionKey(urn);
                if (procKey != null)
                {
                    typeOf.setRole(urn);
                    var procId = idEncoders.getProcedureIdEncoder().encodeID(procKey.getInternalID());
                    typeOf.setHref(ctx.getApiRootURL() + "/" + ProcedureHandler.NAMES[0] + "/" + procId);
                }
            }
        }
    }
}
