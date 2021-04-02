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
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.procedure.ProcedureObsTransactionHandler;
import org.sensorhub.impl.procedure.ProcedureUtils;
import org.sensorhub.impl.procedure.wrapper.ProcedureWrapper;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHandler;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;


public class ProcedureHandler extends AbstractFeatureHandler<IProcedureWithDesc, ProcedureFilter, ProcedureFilter.Builder, IProcedureStore>
{
    public static final int EXTERNAL_ID_SEED = 21933547;
    public static final String[] NAMES = { "procedures" };
    
    ProcedureObsTransactionHandler transactionHandler;
    
    
    public ProcedureHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getProcedureStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.transactionHandler = new ProcedureObsTransactionHandler(eventBus, db);
    }


    @Override
    protected ResourceBinding<FeatureKey, IProcedureWithDesc> getBinding(ResourceContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.isOneOf(ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new ProcedureBindingGeoJson(ctx, idEncoder, forReading);
        else if (format.equals(ResourceFormat.SML_JSON))
            return new ProcedureBindingSmlJson(ctx, idEncoder, forReading);
        else
            throw new InvalidRequestException(UNSUPPORTED_FORMAT_ERROR_MSG + format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.contains(internalID);
    }
    
    
    @Override
    protected IResourceHandler getSubResource(ResourceContext ctx, String id)
    {
        var handler = super.getSubResource(ctx, id);
        if (handler == null)
            return null;
        
        return handler;
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final ProcedureFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        // parent ID
        var ids = parseResourceIds("parentId", queryParams);
        if (ids != null && !ids.isEmpty())
            builder.withParents().withInternalIDs(ids).done();
        
        // parent UID
        var uids = parseMultiValuesArg("parentUid", queryParams);
        if (uids != null && !uids.isEmpty())
            builder.withParents().withUniqueIDs(uids).done();
    }


    @Override
    protected void validate(IProcedureWithDesc resource)
    {
        // TODO Auto-generated method stub
        
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
        
        var procHandler = transactionHandler.addProcedure(res);

        // also add datastreams if outputs were specified in SML description
        if (sml != null)
            ProcedureUtils.addDatastreamsFromOutputs(procHandler, sml.getOutputList());
        
        return procHandler.getProcedureKey();
    }
    
    
    @Override
    protected boolean updateEntry(final ResourceContext ctx, final FeatureKey key, IProcedureWithDesc res) throws DataStoreException
    {        
        var procHandler = transactionHandler.getProcedureHandler(key.getInternalID());
        if (procHandler == null)
            return false;
        
        // cleanup sml description before storage
        var sml = res.getFullDescription();
        if (sml != null)
        {
            res = new ProcedureWrapper(res.getFullDescription())
                .hideOutputs()
                .hideTaskableParams()
                .defaultToValidFromNow();
        }
        
        return procHandler.update(res);
    }
    
    
    protected boolean deleteEntry(final ResourceContext ctx, final FeatureKey key) throws DataStoreException
    {        
        var procHandler = transactionHandler.getProcedureHandler(key.getInternalID());
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
