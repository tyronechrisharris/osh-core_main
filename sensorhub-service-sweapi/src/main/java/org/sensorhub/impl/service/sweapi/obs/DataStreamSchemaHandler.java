/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.util.Asserts;


public class DataStreamSchemaHandler extends ResourceHandler<DataStreamKey, IDataStreamInfo, DataStreamFilter, DataStreamFilter.Builder, IDataStreamStore>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    public static final String[] NAMES = { "schema" };
    
    
    public DataStreamSchemaHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getDataStreamStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
    }
    
    
    @Override
    protected ResourceBinding<DataStreamKey, IDataStreamInfo> getBinding(ResourceContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.JSON))
            return new DataStreamSchemaBindingJson(ctx, idEncoder, forReading);
        else
            throw new InvalidRequestException(UNSUPPORTED_FORMAT_ERROR_MSG + format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.containsKey(new DataStreamKey(internalID));
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
            return handleAuthException(ctx, e);
        }
    }
    
    
    @Override
    protected boolean getById(final ResourceContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
                
        ResourceRef parent = ctx.getParentRef();
        Asserts.checkNotNull(parent, "parent");
        
        // get resource key
        var key = getKey(parent.internalID);
        if (key != null)
            return getByKey(ctx, key);
        else
            return ctx.sendError(404, String.format(NOT_FOUND_ERROR_MSG, id));
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final DataStreamFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
    }


    @Override
    protected void validate(IDataStreamInfo resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    protected DataStreamKey addEntry(final ResourceContext ctx, final IDataStreamInfo res) throws DataStoreException
    {        
        return null;
    }
    
    
    @Override
    protected boolean updateEntry(final ResourceContext ctx, final DataStreamKey key, final IDataStreamInfo res) throws DataStoreException
    {        
        return false;
    }
    
    
    protected boolean deleteEntry(final ResourceContext ctx, final DataStreamKey key) throws DataStoreException
    {        
        return false;
    }


    @Override
    protected DataStreamKey getKey(long publicID)
    {
        return new DataStreamKey(publicID);
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
