/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.feature;

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.procedure.ProcedureObsTransactionHandler;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.procedure.ProcedureHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.vast.ogc.gml.IGeoFeature;


public class FoiHandler extends AbstractFeatureHandler<IGeoFeature, FoiFilter, FoiFilter.Builder, IFoiStore>
{
    public static final int EXTERNAL_ID_SEED = 433584715;
    public static final String[] NAMES = { "featuresOfInterest", "fois" };
    
    ProcedureObsTransactionHandler transactionHandler;
    
    
    public FoiHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getFoiStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.transactionHandler = new ProcedureObsTransactionHandler(eventBus, db);
    }


    @Override
    protected ResourceBinding<FeatureKey, IGeoFeature> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.isOneOf(ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new FeatureBindingGeoJson(ctx, idEncoder, forReading);
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
        if (ctx.isEndOfPath() &&
            !(ctx.getParentRef().type instanceof ProcedureHandler))
            throw ServiceErrors.unsupportedOperation("Features of interest can only be created within a procedure's fois sub-collection");
        
        super.doPost(ctx);
    }


    @Override
    protected void buildFilter(ResourceRef parent, Map<String, String[]> queryParams, FoiFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        if (parent.internalID > 0)
        {
            builder.withParents()
                .withInternalIDs(parent.internalID)
                .includeMembers(true)
                .done();
        }
        else
        {        
            // parent ID
            var ids = parseResourceIds("parentId", queryParams);
            if (ids != null && !ids.isEmpty())
                builder.withParents().withInternalIDs(ids).done();
            
            // parent UID
            var uids = parseMultiValuesArg("parentUid", queryParams);
            if (uids != null && !uids.isEmpty())
                builder.withParents().withUniqueIDs(uids).done();
        }
    }


    @Override
    protected void validate(IGeoFeature resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    protected FeatureKey addEntry(final RequestContext ctx, final IGeoFeature foi) throws DataStoreException
    {        
        var procHandler = transactionHandler.getProcedureHandler(ctx.getParentID());        
        return procHandler.addOrUpdateFoi(foi);
    }


    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
