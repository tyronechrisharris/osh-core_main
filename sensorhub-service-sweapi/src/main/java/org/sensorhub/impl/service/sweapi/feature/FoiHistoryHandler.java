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
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.vast.ogc.gml.IGeoFeature;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;


public class FoiHistoryHandler extends AbstractFeatureHistoryHandler<IGeoFeature, FoiFilter, FoiFilter.Builder, IFoiStore>
{
    
    public FoiHistoryHandler(IEventBus eventBus, IProcedureObsDatabase db, ResourcePermissions permissions)
    {
        super(db.getFoiStore(), new IdEncoder(FoiHandler.EXTERNAL_ID_SEED), permissions);
    }


    @Override
    protected ResourceBinding<FeatureKey, IGeoFeature> getBinding(ResourceContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.isOneOf(ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new FeatureBindingGeoJson(ctx, idEncoder, forReading);
        else
            throw new InvalidRequestException(UNSUPPORTED_FORMAT_ERROR_MSG + format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.contains(internalID);
    }
    

    @Override
    protected void validate(IGeoFeature resource)
    {        
    }
}
