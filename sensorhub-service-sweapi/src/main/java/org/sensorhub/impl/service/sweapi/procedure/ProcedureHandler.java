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
import org.sensorhub.api.database.IFeatureDatabase;
import org.sensorhub.api.datastore.feature.FeatureFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.system.SystemBindingGeoJson;
import org.sensorhub.impl.service.sweapi.system.SystemBindingSmlJson;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.system.SystemDatabaseTransactionHandler;
import org.vast.ogc.gml.IFeature;


public class ProcedureHandler extends AbstractFeatureHandler<IFeature, FeatureFilter, FeatureFilter.Builder, IFeatureStore>
{
    public static final int EXTERNAL_ID_SEED = 342178536;
    public static final String[] NAMES = { "procedures" };
    
    final IEventBus eventBus;
    final SystemDatabaseTransactionHandler transactionHandler;
    final ProcedureEventsHandler eventsHandler;
    
    
    public ProcedureHandler(IEventBus eventBus, IFeatureDatabase db, ResourcePermissions permissions)
    {
        super(db.getFeatureStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.transactionHandler = null;
        this.eventBus = eventBus;
        this.eventsHandler = new ProcedureEventsHandler(eventBus, db, permissions);
        addSubResource(eventsHandler);
    }


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected ResourceBinding<FeatureKey, IFeature> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return (ResourceBinding)new SystemBindingGeoJson(ctx, idEncoder, forReading);
        else if (format.equals(ResourceFormat.SML_JSON))
            return (ResourceBinding)new SystemBindingSmlJson(ctx, idEncoder, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected void subscribeToEvents(final RequestContext ctx) throws InvalidRequestException, IOException
    {
        eventsHandler.doGet(ctx);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.contains(internalID);
    }


    @Override
    protected void validate(IFeature resource)
    {
        // TODO Auto-generated method stub
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
