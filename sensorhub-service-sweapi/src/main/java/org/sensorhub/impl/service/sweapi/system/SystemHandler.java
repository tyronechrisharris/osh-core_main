/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.system;

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHandler;
import org.sensorhub.impl.service.sweapi.procedure.SmlFeatureBindingSmlJson;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.impl.system.SystemDatabaseTransactionHandler;
import org.sensorhub.impl.system.SystemUtils;
import org.sensorhub.impl.system.wrapper.SmlFeatureWrapper;


public class SystemHandler extends AbstractFeatureHandler<ISystemWithDesc, SystemFilter, SystemFilter.Builder, ISystemDescStore>
{
    public static final String[] NAMES = { "systems" };
    
    final IEventBus eventBus;
    final IObsSystemDatabase db;
    final SystemDatabaseTransactionHandler transactionHandler;
    final SystemEventsHandler eventsHandler;
    
    
    public SystemHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getReadDb().getSystemDescStore(), db.getSystemIdEncoder(), db.getIdEncoders(), permissions);
        this.db = db.getReadDb();
        this.eventBus = eventBus;
        this.transactionHandler = new SystemDatabaseTransactionHandler(eventBus, db.getWriteDb());
        
        this.eventsHandler = new SystemEventsHandler(eventBus, db, permissions);
        addSubResource(eventsHandler);
    }


    @Override
    protected ResourceBinding<FeatureKey, ISystemWithDesc> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest())
            return new SystemBindingHtml(ctx, idEncoders, true, db);
        else if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new SystemBindingGeoJson(ctx, idEncoders, forReading);
        else if (format.equals(ResourceFormat.SML_JSON))
            return new SmlFeatureBindingSmlJson<ISystemWithDesc>(ctx, idEncoders, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected void subscribeToEvents(final RequestContext ctx) throws InvalidRequestException, IOException
    {
        eventsHandler.doGet(ctx);
    }
    
    
    @Override
    protected boolean isValidID(BigId internalID)
    {
        return dataStore.contains(internalID);
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final SystemFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        var val = getSingleParam("searchMembers", queryParams);
        boolean searchMembers =  (val != null && !val.equalsIgnoreCase("false"));
        boolean parentSelected = false;
        
        // parent ID
        var ids = parseResourceIds("parentId", queryParams, idEncoders.getSystemIdEncoder());
        if (ids != null && !ids.isEmpty())
        {
            parentSelected = true;
            builder.withParents()
                .withInternalIDs(ids)
                .done();
        } 
        
        // parent UID
        var uids = parseMultiValuesArg("parentUid", queryParams);
        if (uids != null && !uids.isEmpty())
        {
            parentSelected = true;
            builder.withParents()
                .withUniqueIDs(uids)
                .done();
        }
        
        if (!parentSelected && !searchMembers)
            builder.withNoParent();
    }


    @Override
    protected void validate(ISystemWithDesc resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    protected FeatureKey addEntry(final RequestContext ctx, ISystemWithDesc res) throws DataStoreException
    {        
        // cleanup sml description before storage
        var sml = res.getFullDescription();
        if (sml != null)
        {
            res = new SmlFeatureWrapper(res.getFullDescription())
                .hideOutputs()
                .hideTaskableParams()
                .defaultToValidFromNow();
        }
        
        var sysHandler = transactionHandler.addSystem(res);

        // also add datastreams if outputs were specified in SML description
        if (sml != null)
            SystemUtils.addDatastreamsFromOutputs(sysHandler, sml.getOutputList());
        
        return sysHandler.getSystemKey();
    }
    
    
    @Override
    protected boolean updateEntry(final RequestContext ctx, final FeatureKey key, ISystemWithDesc res) throws DataStoreException
    {        
        var sysHandler = transactionHandler.getSystemHandler(key.getInternalID());
        if (sysHandler == null)
            return false;
        
        // cleanup sml description before storage
        var sml = res.getFullDescription();
        if (sml != null)
        {
            res = new SmlFeatureWrapper(res.getFullDescription())
                .hideOutputs()
                .hideTaskableParams()
                .defaultToValidFromNow();
        }
        
        return sysHandler.update(res);
    }
    
    
    protected boolean deleteEntry(final RequestContext ctx, final FeatureKey key) throws DataStoreException
    {
        var sysHandler = transactionHandler.getSystemHandler(key.getInternalID());
        if (sysHandler == null)
            return false;
        else
        {
            var paramStr = ctx.getParameter("cascade");
            var deleteNested = paramStr != null ? Boolean.parseBoolean(paramStr) : false;
            return sysHandler.delete(deleteNested);
        }
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
