/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.namespace.QName;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.datastore.procedure.IProcedureStore.ProcedureField;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;
import org.sensorhub.api.procedure.IProcedureWithDesc;


/**
 * <p>
 * Procedure description store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVProcedureStoreImpl extends MVBaseFeatureStoreImpl<IProcedureWithDesc, ProcedureField, ProcedureFilter> implements IProcedureStore
{
    IDataStreamStore dataStreamStore;
    
    
    protected MVProcedureStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing procedure store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVProcedureStoreImpl open(MVStore mvStore, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        return (MVProcedureStoreImpl)new MVProcedureStoreImpl().init(mvStore, dataStoreInfo, null);
    }
    
    
    @Override
    protected Stream<Entry<MVFeatureParentKey, IProcedureWithDesc>> getIndexedStream(ProcedureFilter filter)
    {
        var resultStream = super.getIndexedStream(filter);
        
        if (filter.getParentFilter() != null)
        {
            if (resultStream == null)
            {
                resultStream = selectParentIDs(filter.getParentFilter())
                    .flatMap(id -> getParentResultStream(id, filter.getValidTime()));
            }
            else
            {
                var parentIDs = selectParentIDs(filter.getParentFilter())
                    .collect(Collectors.toSet());
                
                resultStream = resultStream.filter(
                    e -> parentIDs.contains(((MVFeatureParentKey)e.getKey()).getParentID()));
                
                // post filter using keys valid time if needed
                if (filter.getValidTime() != null)
                    resultStream = postFilterKeyValidTime(resultStream, filter.getValidTime());
            }
        }
        
        return resultStream;
    }


    @Override
    public Stream<Entry<FeatureKey, IProcedureWithDesc>> selectEntries(ProcedureFilter filter, Set<ProcedureField> fields)
    {
        // update validTime in the case it ends at now and there is a
        // more recent version of the procedure description available
        Stream<Entry<FeatureKey, IProcedureWithDesc>> resultStream = super.selectEntries(filter, fields).map(e -> {
            var proc = (IProcedureWithDesc)e.getValue();
            var procWrap = new IProcedureWithDesc()
            {
                public String getId() { return proc.getId(); }
                public String getUniqueIdentifier() { return proc.getUniqueIdentifier(); }
                public String getName() { return proc.getName(); }
                public String getDescription() { return proc.getDescription(); }
                public Map<QName, Object> getProperties() { return proc.getProperties(); }
                public AbstractProcess getFullDescription() { return proc.getFullDescription(); }  
                
                public TimeExtent getValidTime()
                {
                    var nextKey = featuresIndex.higherKey((MVFeatureParentKey)e.getKey());
                    if (nextKey != null && nextKey.getInternalID() == e.getKey().getInternalID() &&
                        proc.getValidTime() != null && proc.getValidTime().endsNow())
                        return TimeExtent.period(proc.getValidTime().begin(), nextKey.getValidStartTime());
                    else
                        return proc.getValidTime();
                }                  
            };
            
            return new DataUtils.MapEntry<FeatureKey, IProcedureWithDesc>(e.getKey(), procWrap);
        });
        
        // apply post filter on time now that we computed the correct valid time period
        if (filter.getValidTime() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        return resultStream;
    }


    @Override
    public void linkTo(IDataStreamStore dataStreamStore)
    {
        Asserts.checkNotNull(dataStreamStore, IDataStreamStore.class);
        
        if (this.dataStreamStore != dataStreamStore)
        {
            this.dataStreamStore = dataStreamStore;
            dataStreamStore.linkTo(this);
        }
    }    

}
