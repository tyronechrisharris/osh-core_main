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
import org.sensorhub.impl.service.sweapi.procedure.ProcedureHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.ogc.gml.IGeoFeature;


public class FoiHandler extends AbstractFeatureHandler<IGeoFeature, FoiFilter, FoiFilter.Builder, IFoiStore>
{
    public static final String[] NAMES = { "featuresOfInterest", "fois" };
    
    ProcedureObsTransactionHandler transactionHandler;
    
    
    public FoiHandler(IEventBus eventBus, ProcedureObsDbWrapper db)
    {
        super(db.getFoiStore(), new IdEncoder(FeatureHandler.EXTERNAL_ID_SEED));
        this.transactionHandler = new ProcedureObsTransactionHandler(eventBus, db);
    }


    @Override
    protected ResourceBinding<FeatureKey, IGeoFeature> getBinding(ResourceContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.isOneOf(ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new FeatureFormatterGeoJson(ctx, idEncoder, forReading);
        else
            throw new InvalidRequestException(UNSUPPORTED_FORMAT_ERROR_MSG + format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.contains(internalID);
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEmpty() &&
            !(ctx.getParentRef().type instanceof ProcedureHandler))
            return ctx.sendError(405, "Features of interest can only be created within a procedure's fois sub-collection");
        
        return super.doPost(ctx);
    }


    @Override
    protected void buildFilter(ResourceRef parent, Map<String, String[]> queryParams, FoiFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        if (parent.internalID > 0)
        {
            /*builder.withObservations()
                .withProcedures(parent.internalID)
                .done();*/
            builder.withParents(parent.internalID);
        }
    }


    @Override
    protected void validate(IGeoFeature resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    protected FeatureKey addEntry(final ResourceContext ctx, final IGeoFeature foi) throws DataStoreException
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
