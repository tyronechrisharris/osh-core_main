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
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.impl.procedure.ProcedureObsTransactionHandler;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ProcedureObsDbWrapper;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.procedure.ProcedureHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;


public class DataStreamHandler extends ResourceHandler<DataStreamKey, IDataStreamInfo, DataStreamFilter, DataStreamFilter.Builder, IDataStreamStore>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    public static final String[] NAMES = { "datastreams" };
    
    ProcedureObsTransactionHandler transactionHandler;
    
    
    public DataStreamHandler(IEventBus eventBus, ProcedureObsDbWrapper db, ResourcePermissions permissions)
    {
        super(db.getDataStreamStore(), new IdEncoder(EXTERNAL_ID_SEED), permissions);
        this.transactionHandler = new ProcedureObsTransactionHandler(eventBus, db);
    }
    
    
    @Override
    protected ResourceBinding<DataStreamKey, IDataStreamInfo> getBinding(ResourceContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        
        if (format.equals(ResourceFormat.JSON))
            return new DataStreamBindingJson(ctx, idEncoder, forReading);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    protected boolean isValidID(long internalID)
    {
        return dataStore.containsKey(new DataStreamKey(internalID));
    }
    
    
    @Override
    public void doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEndOfPath() &&
            !(ctx.getParentRef().type instanceof ProcedureHandler))
            throw ServiceErrors.unsupportedOperation("Datastreams can only be created within a Procedure");
        
        super.doPost(ctx);
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final DataStreamFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        // only fetch datastreams valid at current time by default
        builder.withCurrentVersion();
        
        // filter on parent if needed
        if (parent.internalID > 0)
            builder.withProcedures(parent.internalID);
        
        // foi param
        var foiIDs = parseResourceIds("foi", queryParams);
        if (foiIDs != null && !foiIDs.isEmpty())
            builder.withFois().withInternalIDs(foiIDs);
    }


    @Override
    protected void validate(IDataStreamInfo resource)
    {
        // TODO Auto-generated method stub
        
    }
    
    
    @Override
    protected DataStreamKey addEntry(final ResourceContext ctx, final IDataStreamInfo res) throws DataStoreException
    {        
        var procID = ctx.getParentID();
        var procHandler = transactionHandler.getProcedureHandler(procID);
        
        var dsHandler = procHandler.addOrUpdateDataStream(res.getOutputName(), res.getRecordStructure(), res.getRecordEncoding());
        var dsKey = dsHandler.getDataStreamKey();
        
        return new DataStreamKey(dsKey.getInternalID());
    }
    
    
    @Override
    protected boolean updateEntry(final ResourceContext ctx, final DataStreamKey key, final IDataStreamInfo res) throws DataStoreException
    {        
        var dsID = key.getInternalID();
        var dsHandler = transactionHandler.getDataStreamHandler(dsID);
        
        if (dsHandler == null)
            return false;
        else
            return dsHandler.update(res.getRecordStructure(), res.getRecordEncoding());
    }
    
    
    protected boolean deleteEntry(final ResourceContext ctx, final DataStreamKey key) throws DataStoreException
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
