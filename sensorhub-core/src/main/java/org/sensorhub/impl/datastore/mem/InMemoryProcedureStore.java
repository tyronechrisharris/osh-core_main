/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.mem;

import java.util.stream.Stream;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.datastore.procedure.IProcedureStore.ProcedureField;
import org.vast.util.Asserts;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.datastore.DataStoreUtils;


/**
 * <p>
 * In-memory implementation of procedure store backed by a {@link java.util.NavigableMap}.
 * This implementation is only used to store the latest procedure state and thus
 * doesn't support procedure description history.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryProcedureStore extends InMemoryBaseFeatureStore<IProcedureWithDesc, ProcedureField, ProcedureFilter> implements IProcedureStore
{
    IDataStreamStore dataStreamStore;
    
    
    public InMemoryProcedureStore()
    {
        this.idProvider = new HashCodeFeatureIdProvider(1353704900);
    }
    
    
    public InMemoryProcedureStore(IdProvider<? super IProcedureWithDesc> idProvider)
    {
        this.idProvider = Asserts.checkNotNull(idProvider, IdProvider.class);            
    }
    
    
    @Override
    protected Stream<Entry<FeatureKey, IProcedureWithDesc>> getIndexedStream(ProcedureFilter filter)
    {
        var resultStream = super.getIndexedStream(filter);
        
        if (resultStream == null)
        {
            if (filter.getParentFilter() != null)
            {
                var parentIDStream = DataStoreUtils.selectProcedureIDs(this, filter.getParentFilter());
                return postFilterOnParents(resultStream, parentIDStream);
            }
            
            else if (filter.getDataStreamFilter() != null)
            {
                DataStoreUtils.selectDataStreams(dataStreamStore, filter.getDataStreamFilter())
                    .map(ds -> ds.getProcedureID().getInternalID())
                    .distinct()
                    .map(id -> map.get(new FeatureKey(id)));
            }
        }
        
        return resultStream;
    }
    
    
    @Override
    public void linkTo(IDataStreamStore dataStreamStore)
    {
        this.dataStreamStore = Asserts.checkNotNull(dataStreamStore, IDataStreamStore.class);
    }
}
