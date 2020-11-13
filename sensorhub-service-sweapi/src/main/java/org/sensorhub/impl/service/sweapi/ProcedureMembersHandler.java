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
import java.util.Map;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.ResourceContext.ResourceRef;


public class ProcedureMembersHandler extends AbstractFeatureHandler<IProcedureWithDesc, ProcedureFilter, ProcedureFilter.Builder, IProcedureStore>
{
    public static final String[] NAMES = { "members" };
    
    
    public ProcedureMembersHandler(IProcedureStore dataStore)
    {
        super(dataStore, new ProcedureResourceType());
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEmpty() &&
            //!(ctx.getParentRef().type instanceof ProcedureGroupResourceType) &&
            !(ctx.getParentRef().type instanceof ProcedureResourceType))
            return ctx.sendError(405, "Procedures can only be created within a ProcedureGroup");
        
        return super.doPost(ctx);
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final ProcedureFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        // filter on parent if needed
        if (parent.internalID > 0)
        {
            builder.withParents()
                .withInternalIDs(parent.internalID)
                .done();
        }
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }


    @Override
    protected void validate(IProcedureWithDesc resource)
    {
        // TODO Auto-generated method stub
        
    }
}
