/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext.ResourceRef;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.system.SystemDatabaseTransactionHandler;
import org.vast.util.Asserts;


public class DataStreamHandler extends ResourceHandler<DataStreamKey, IDataStreamInfo, DataStreamFilter, DataStreamFilter.Builder, IDataStreamStore>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    public static final String[] NAMES = { "datastreams" };
    
    final IEventBus eventBus;
    final SystemDatabaseTransactionHandler transactionHandler;
    final DataStreamEventsHandler eventsHandler;
    final Map<String, CustomObsFormat> customFormats;
    
    
    public DataStreamHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions, Map<String, CustomObsFormat> customFormats)
    {
        super(db.getDataStreamStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.eventBus = eventBus;
        this.transactionHandler = new SystemDatabaseTransactionHandler(eventBus, db);
        this.eventsHandler = new DataStreamEventsHandler(eventBus, db, permissions);
        this.customFormats = Asserts.checkNotNull(customFormats, "customFormats");
    }
    
    
    @Override
    protected ResourceBinding<DataStreamKey, IDataStreamInfo> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.JSON))
            return new DataStreamBindingJson(ctx, idEncoder, forReading, customFormats);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws IOException
    {
        if (ctx.isEndOfPath() &&
            !(ctx.getParentRef().type instanceof SystemHandler))
            throw ServiceErrors.unsupportedOperation("Datastreams can only be created within a System resource");
        
        super.doPost(ctx);
    }
    
    
    @Override
    protected void subscribeToEvents(final RequestContext ctx) throws InvalidRequestException, IOException
    {
        eventsHandler.doGet(ctx);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.containsKey(new DataStreamKey(internalID));
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final DataStreamFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        // only fetch datastreams valid at current time by default
        builder.withCurrentVersion();
        
        // filter on parent if needed
        if (parent.internalID > 0)
        {
            builder.withSystems()
                .withInternalIDs(parent.internalID)
                .includeMembers(true)
                .done();
        }
        
        // foi param
        var foiIDs = parseResourceIds("foi", queryParams);
        if (foiIDs != null && !foiIDs.isEmpty())
        {
            builder.withFois()
                .withInternalIDs(foiIDs)
                .done();
        }
    }


    @Override
    protected void validate(IDataStreamInfo resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    protected DataStreamKey addEntry(final RequestContext ctx, final IDataStreamInfo res) throws DataStoreException
    {        
        var sysID = ctx.getParentID();
        var procHandler = transactionHandler.getSystemHandler(sysID);
        
        var dsHandler = procHandler.addOrUpdateDataStream(res.getOutputName(), res.getRecordStructure(), res.getRecordEncoding());
        var dsKey = dsHandler.getDataStreamKey();
        
        return new DataStreamKey(dsKey.getInternalID());
    }
    
    
    @Override
    protected boolean updateEntry(final RequestContext ctx, final DataStreamKey key, final IDataStreamInfo res) throws DataStoreException
    {        
        var dsID = key.getInternalID();
        var dsHandler = transactionHandler.getDataStreamHandler(dsID);
        
        if (dsHandler == null)
            return false;
        else
            return dsHandler.update(res.getRecordStructure(), res.getRecordEncoding());
    }
    
    
    protected boolean deleteEntry(final RequestContext ctx, final DataStreamKey key) throws DataStoreException
    {        
        var dsID = key.getInternalID();
        
        var dsHandler = transactionHandler.getDataStreamHandler(dsID);
        if (dsHandler == null)
            return false;
        else
            return dsHandler.delete();
    }


    @Override
    protected DataStreamKey getKey(long publicID)
    {
        return new DataStreamKey(publicID);
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
}
