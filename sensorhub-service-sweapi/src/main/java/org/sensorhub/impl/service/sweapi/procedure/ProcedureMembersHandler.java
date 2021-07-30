/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.procedure;

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.procedure.ProcedureUtils;
import org.sensorhub.impl.procedure.wrapper.ProcedureWrapper;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;


public class ProcedureMembersHandler extends ProcedureHandler
{
    public static final String[] NAMES = { "members" };
    
    
    public ProcedureMembersHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(eventBus, db, permissions);
    }
    
    
    @Override
    public void doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEndOfPath() &&
            !(ctx.getParentRef().type instanceof ProcedureHandler))
            throw ServiceErrors.unsupportedOperation("Procedures can only be created within a ProcedureGroup");
        
        super.doPost(ctx);
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
    protected FeatureKey addEntry(final ResourceContext ctx, IProcedureWithDesc res) throws DataStoreException
    {        
        // cleanup sml description before storage
        var sml = res.getFullDescription();
        if (sml != null)
        {
            res = new ProcedureWrapper(res.getFullDescription())
                .hideOutputs()
                .hideTaskableParams()
                .defaultToValidFromNow();
        }        
        
        var groupID = ctx.getParentID();
        var parentHandler = transactionHandler.getProcedureHandler(groupID);
        if (parentHandler == null)
            return null;
        var procHandler = parentHandler.addMember(res);

        // also add datastreams if outputs were specified in SML description
        if (sml != null)
            ProcedureUtils.addDatastreamsFromOutputs(procHandler, sml.getOutputList());
        
        return procHandler.getProcedureKey();
    }
    
    
    @Override
    protected String getCanonicalResourceUrl(final String id)
    {
        return "/" + ProcedureHandler.NAMES[0] + "/" + id;
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
