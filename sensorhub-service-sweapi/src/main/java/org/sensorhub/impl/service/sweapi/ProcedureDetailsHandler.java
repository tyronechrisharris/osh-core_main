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
import org.sensorhub.api.procedure.ProcedureWrapper;
import org.sensorhub.impl.service.sweapi.ResourceContext.ResourceRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import net.opengis.sensorml.v20.AbstractProcess;


public class ProcedureDetailsHandler extends AbstractFeatureHandler<IProcedureWithDesc, ProcedureFilter, ProcedureFilter.Builder, IProcedureStore>
{
    static final Logger log = LoggerFactory.getLogger(ProcedureDetailsHandler.class);
    public static final String[] NAMES = { "details", "specsheet" }; //"fullDescription"; //"specs"; //"specsheet"; //"metadata";
    
    
    public ProcedureDetailsHandler(IProcedureStore dataStore)
    {
        super(dataStore, new ProcedureDetailsResourceType());
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        return ctx.sendError(405, "Cannot POST here, use PUT on main resource URL");
    }
    
    
    @Override
    public boolean doPut(final ResourceContext ctx) throws IOException
    {
        return ctx.sendError(405, "Cannot PUT here, use PUT on main resource URL");
    }
    
    
    @Override
    public boolean doDelete(final ResourceContext ctx) throws IOException
    {
        // TODO implement DELETE for details only?
        return ctx.sendError(405, "Cannot DELETE here, use DELETE on main resource URL");
    }
    
    
    @Override
    public boolean doGet(ResourceContext ctx) throws IOException
    {
        try
        {
            if (ctx.isEmpty())
                return getById(ctx, "");
            else
                return ctx.sendError(404, "Invalid resource URL");
        }
        catch (InvalidRequestException e)
        {
            return ctx.sendError(400, e.getMessage());
        }
        catch (SecurityException e)
        {
            return ctx.sendError(403, ACCESS_DENIED_ERROR_MSG);
        }
    }
    
    
    @Override
    protected boolean getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        ResourceRef parent = ctx.getParentRef();
        Asserts.checkNotNull(parent, "parent");
        
        // internal ID & version number
        long internalID = parent.internalID;
        long version = parent.version;
        if (version < 0)
            return false;
        
        var key = getKey(internalID, version);
        final AbstractProcess sml = dataStore.get(key).getFullDescription();
        if (sml == null)
            return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
        
        var queryParams = ctx.req.getParameterMap();
        ctx.resp.setStatus(200);
        resourceType.serialize(key, new ProcedureWrapper(sml), parseSelectArg(queryParams), null, ctx.resp.getOutputStream());
        
        return true;
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final ProcedureFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        // TODO implement select by sections
    }


    @Override
    protected void validate(IProcedureWithDesc resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
