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
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;


public class ProcedureHandler extends AbstractFeatureHandler<IProcedureWithDesc, ProcedureFilter, ProcedureFilter.Builder, IProcedureStore>
{
    public static final int EXTERNAL_ID_SEED = 342178536;
    public static final String[] NAMES = { "procedures" };
    
    final IProcedureDatabase readDb;
    final IProcedureDatabase writeDb;
    final IEventBus eventBus;
    final ProcedureEventsHandler eventsHandler;
    final IdConverter idConverter;
    
    public ProcedureHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super(((IProcedureDatabase)db.getReadDb()).getProcedureStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.readDb = (IProcedureDatabase)db.getReadDb();
        this.writeDb = db.getWriteDb() instanceof IProcedureDatabase ? (IProcedureDatabase)db.getWriteDb() : null;
        if (writeDb == null)
            readOnly = true;
        this.idConverter = db.getIdConverter();
        this.eventBus = eventBus;
        
        this.eventsHandler = new ProcedureEventsHandler(eventBus, this.readDb, permissions);
        addSubResource(eventsHandler);
    }


    @Override
    protected ResourceBinding<FeatureKey, IProcedureWithDesc> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest())
        {
            var title = ctx.getParentID() != 0 ? "Components of {}" : "All Procedures";
            return new ProcedureBindingHtml(ctx, idEncoder, true, title, readDb);
        }
        else if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON, ResourceFormat.GEOJSON))
            return new ProcedureBindingGeoJson(ctx, idEncoder, forReading);
        else if (format.equals(ResourceFormat.SML_JSON))
            return new SmlFeatureBindingSmlJson<IProcedureWithDesc>(ctx, idEncoder, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected FeatureKey addEntry(RequestContext ctx, IProcedureWithDesc res) throws DataStoreException
    {
        return writeDb.getProcedureStore().add(res);
    }


    @Override
    protected boolean updateEntry(RequestContext ctx, FeatureKey key, IProcedureWithDesc res) throws DataStoreException
    {
        var writeID = idConverter.toInternalID(key.getInternalID());
        var writeKey = new FeatureKey(writeID, key.getValidStartTime());
        return writeDb.getProcedureStore().computeIfPresent(writeKey, (k,v) -> res) != null;
    }


    @Override
    protected boolean deleteEntry(RequestContext ctx, FeatureKey key) throws DataStoreException
    {
        var writeID = idConverter.toInternalID(key.getInternalID());
        var writeKey = new FeatureKey(writeID, key.getValidStartTime());
        return writeDb.getProcedureStore().remove(writeKey) != null;
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
