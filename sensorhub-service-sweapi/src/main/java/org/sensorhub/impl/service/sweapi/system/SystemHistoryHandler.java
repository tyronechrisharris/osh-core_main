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
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHistoryHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;


public class SystemHistoryHandler extends AbstractFeatureHistoryHandler<ISystemWithDesc, SystemFilter, SystemFilter.Builder, ISystemDescStore>
{
    final IObsSystemDatabase db;
    
    
    public SystemHistoryHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getReadDb().getSystemDescStore(), new IdEncoder(SystemHandler.EXTERNAL_ID_SEED), permissions);
        this.db = db.getReadDb();
    }


    @Override
    protected ResourceBinding<FeatureKey, ISystemWithDesc> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest())
            return new SystemBindingHtml(ctx, idEncoder, true, "History of {}", db);
        else if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new SystemBindingGeoJson(ctx, idEncoder, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.contains(internalID);
    }
    

    @Override
    protected void validate(ISystemWithDesc resource)
    {        
    }
}
