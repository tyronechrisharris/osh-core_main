/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.task;

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.vast.util.Asserts;


public class CommandStreamSchemaHandler extends ResourceHandler<CommandStreamKey, ICommandStreamInfo, CommandStreamFilter, CommandStreamFilter.Builder, ICommandStreamStore>
{
    public static final String[] NAMES = { "schema" };
    
    
    public CommandStreamSchemaHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getReadDb().getCommandStreamStore(), db.getCommandStreamIdEncoder(), db.getIdEncoders(), permissions);
    }
    
    
    @Override
    protected ResourceBinding<CommandStreamKey, ICommandStreamInfo> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        if (!format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON))
            throw ServiceErrors.unsupportedFormat(format);
        
        // generate proper schema depending on command format
        var cmdFormat = parseFormat("commandFormat", ctx.getParameterMap());
        if (cmdFormat == null)
            cmdFormat = ResourceFormat.JSON;
        
        if (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest())
            return new CommandStreamBindingHtml(ctx, idEncoders);
        else if (cmdFormat.equals(ResourceFormat.JSON))
            return new CommandStreamSchemaBindingJson(ctx, idEncoders, forReading);
        else if (cmdFormat.getMimeType().startsWith(ResourceFormat.SWE_FORMAT_PREFIX))
            return new CommandStreamSchemaBindingSweCommon(cmdFormat, ctx, idEncoders, forReading);
        else
            throw ServiceErrors.unsupportedFormat(cmdFormat);
    }
    
    
    @Override
    protected boolean isValidID(BigId internalID)
    {
        return dataStore.containsKey(new CommandStreamKey(internalID));
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation("Cannot POST here, use PUT on main resource URL");
    }
    
    
    @Override
    public void doPut(final RequestContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation("Cannot PUT here, use PUT on main resource URL");
    }
    
    
    @Override
    public void doDelete(final RequestContext ctx) throws IOException
    {
        throw ServiceErrors.unsupportedOperation("Cannot DELETE here, use DELETE on main resource URL");
    }
    
    
    @Override
    public void doGet(RequestContext ctx) throws IOException
    {
        if (ctx.isEndOfPath())
            getById(ctx, "");
        else
            throw ServiceErrors.badRequest(INVALID_URI_ERROR_MSG);
    }
    
    
    @Override
    protected void getById(final RequestContext ctx, final String id) throws InvalidRequestException, IOException
    {
        // check permissions
        ctx.getSecurityHandler().checkPermission(permissions.read);
        
        ResourceRef parent = ctx.getParentRef();
        Asserts.checkNotNull(parent, "parent");
        
        // get resource key
        var key = getKey(parent.internalID);
        if (key != null)
            getByKey(ctx, key);
        else
            throw ServiceErrors.notFound();
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final CommandStreamFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
    }


    @Override
    protected void validate(ICommandStreamInfo resource)
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    protected CommandStreamKey getKey(BigId publicID)
    {
        return new CommandStreamKey(publicID);
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
