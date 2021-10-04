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
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.impl.system.SystemDatabaseTransactionHandler;
import org.sensorhub.impl.system.SystemUtils;
import org.sensorhub.impl.system.wrapper.SystemWrapper;


public class SystemHandler extends AbstractFeatureHandler<ISystemWithDesc, SystemFilter, SystemFilter.Builder, ISystemDescStore>
{
    public static final int EXTERNAL_ID_SEED = 21933547;
    public static final String[] NAMES = { "systems" };
    
    SystemDatabaseTransactionHandler transactionHandler;
    
    
    public SystemHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getSystemDescStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.transactionHandler = new SystemDatabaseTransactionHandler(eventBus, db);
    }


    @Override
    protected ResourceBinding<FeatureKey, ISystemWithDesc> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.isOneOf(ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new SystemBindingGeoJson(ctx, idEncoder, forReading);
        else if (format.equals(ResourceFormat.SML_JSON))
            return new SystemBindingSmlJson(ctx, idEncoder, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
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
        var ids = parseResourceIds("parentId", queryParams);
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
            res = new SystemWrapper(res.getFullDescription())
                .hideOutputs()
                .hideTaskableParams()
                .defaultToValidFromNow();
        }
        
        var procHandler = transactionHandler.addSystem(res);

        // also add datastreams if outputs were specified in SML description
        if (sml != null)
            SystemUtils.addDatastreamsFromOutputs(procHandler, sml.getOutputList());
        
        return procHandler.getSystemKey();
    }
    
    
    @Override
    protected boolean updateEntry(final RequestContext ctx, final FeatureKey key, ISystemWithDesc res) throws DataStoreException
    {        
        var procHandler = transactionHandler.getSystemHandler(key.getInternalID());
        if (procHandler == null)
            return false;
        
        // cleanup sml description before storage
        var sml = res.getFullDescription();
        if (sml != null)
        {
            res = new SystemWrapper(res.getFullDescription())
                .hideOutputs()
                .hideTaskableParams()
                .defaultToValidFromNow();
        }
        
        return procHandler.update(res);
    }
    
    
    protected boolean deleteEntry(final RequestContext ctx, final FeatureKey key) throws DataStoreException
    {        
        var procHandler = transactionHandler.getSystemHandler(key.getInternalID());
        if (procHandler == null)
            return false;
        else
            return procHandler.delete();
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
