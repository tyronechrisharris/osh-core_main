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
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.impl.system.wrapper.ProcessWrapper;
import org.sensorhub.impl.system.wrapper.SmlFeatureWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.Asserts;
import net.opengis.sensorml.v20.AbstractProcess;


public class ProcedureDetailsHandler extends AbstractFeatureHandler<IProcedureWithDesc, ProcedureFilter, ProcedureFilter.Builder, IProcedureStore>
{
    static final Logger log = LoggerFactory.getLogger(ProcedureDetailsHandler.class);
    public static final String[] NAMES = { "details", "specsheet" };
    
    final IProcedureDatabase db;
    
    
    public ProcedureDetailsHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super(((IProcedureDatabase)db.getReadDb()).getProcedureStore(), new IdEncoder(ProcedureHandler.EXTERNAL_ID_SEED), permissions);
        this.db = (IProcedureDatabase)db.getReadDb();
    }


    @Override
    protected ResourceBinding<FeatureKey, IProcedureWithDesc> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest())
            return new ProcedureBindingHtml(ctx, idEncoder, false, "Specsheet of {}", db);
        else if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON, ResourceFormat.SML_JSON))
            return new SmlFeatureBindingSmlJson<IProcedureWithDesc>(ctx, idEncoder, forReading);
        else if (format.isOneOf(ResourceFormat.APPLI_XML, ResourceFormat.SML_XML))
            return new SmlFeatureBindingSmlXml<IProcedureWithDesc>(ctx, idEncoder, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.contains(internalID);
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
        
        // internal ID & version number
        long internalID = parent.internalID;
        long version = parent.version;
        if (version < 0)
            throw ServiceErrors.badRequest(INVALID_VERSION_ERROR_MSG + version);
        
        var key = getKey(internalID, version);
        AbstractProcess sml = dataStore.get(key).getFullDescription();
        if (sml == null)
            throw ServiceErrors.notFound();
        
        // generate outputs from datastreams
        // + override ID
        var idStr = Long.toString(idEncoder.encodeID(internalID), 36);
        //sml = SystemUtils.addOutputsFromDatastreams(internalID, sml, db.getDataStreamStore())
        //    .withId(idStr);
        sml = ProcessWrapper.getWrapper(sml).withId(idStr);
        
        var queryParams = ctx.getParameterMap();
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        var binding = getBinding(ctx, false);
        
        ctx.setResponseContentType(responseFormat.getMimeType());
        binding.serialize(key, new SmlFeatureWrapper(sml), true);
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
