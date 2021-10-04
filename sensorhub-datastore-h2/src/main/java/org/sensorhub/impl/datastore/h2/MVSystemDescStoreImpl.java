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
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.DataType;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.datastore.system.ISystemDescStore.SystemField;
import org.sensorhub.api.system.ISystemWithDesc;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.hash.Hashing;
import net.opengis.sensorml.v20.AbstractProcess;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.h2.MVDatabaseConfig.IdProviderType;


/**
 * <p>
 * System description store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVSystemDescStoreImpl extends MVBaseFeatureStoreImpl<ISystemWithDesc, SystemField, SystemFilter> implements ISystemDescStore
{
    IDataStreamStore dataStreamStore;
    
    
    protected MVSystemDescStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing system store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param idProviderType Type of ID provider to use to generate new IDs
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVSystemDescStoreImpl open(MVStore mvStore, IdProviderType idProviderType, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        // create ID provider
        IdProvider<ISystemWithDesc> idProvider = null;
        if (idProviderType == IdProviderType.UID_HASH)
        {
            var hashFunc = Hashing.murmur3_128(212158449);
            idProvider = f -> {
                var hc = hashFunc.hashUnencodedChars(f.getUniqueIdentifier());
                return hc.asLong() & 0xFFFFFFFFFFFFL; // keep only 48 bits
            };
        }
        
        return (MVSystemDescStoreImpl)new MVSystemDescStoreImpl().init(mvStore, dataStoreInfo, idProvider);
    }
    
    
    @Override
    protected DataType getFeatureDataType(MVMap<String, Integer> kryoClassMap)
    {
        return new SystemDataType(kryoClassMap);
    }
    
    
    @Override
    protected Stream<Entry<MVFeatureParentKey, ISystemWithDesc>> getIndexedStream(SystemFilter filter)
    {
        var resultStream = super.getIndexedStream(filter);
        
        if (filter.getParentFilter() != null)
        {
            var parentIDStream = DataStoreUtils.selectFeatureIDs(this, filter.getParentFilter());
            
            if (resultStream == null)
            {
                resultStream = parentIDStream
                    .flatMap(id -> getParentResultStream(id, filter.getValidTime()));
            }
            else
            {
                var parentIDs = parentIDStream.collect(Collectors.toSet());
                resultStream = resultStream.filter(
                    e -> parentIDs.contains(((MVFeatureParentKey)e.getKey()).getParentID()));
                
                // post filter using keys valid time if needed
                if (filter.getValidTime() != null)
                    resultStream = postFilterKeyValidTime(resultStream, filter.getValidTime());
            }
        }
        
        if (filter.getDataStreamFilter() != null)
        {
            var sysIDs = DataStoreUtils.selectDataStreams(dataStreamStore, filter.getDataStreamFilter())
                .map(ds -> ds.getSystemID().getInternalID())
                .collect(Collectors.toSet());
            
            if (resultStream == null)
            {            
                return super.getIndexedStream(SystemFilter.Builder.from(filter)
                    .withInternalIDs(sysIDs)
                    .build());
            }
            else
            {
                resultStream = resultStream
                    .filter(e -> sysIDs.contains(e.getKey().getInternalID()));
            }
        }
        
        return resultStream;
    }


    @Override
    public Stream<Entry<FeatureKey, ISystemWithDesc>> selectEntries(SystemFilter filter, Set<SystemField> fields)
    {
        // update validTime in the case it ends at now and there is a
        // more recent version of the system description available
        Stream<Entry<FeatureKey, ISystemWithDesc>> resultStream = super.selectEntries(filter, fields).map(e -> {
            var proc = (ISystemWithDesc)e.getValue();
            var procWrap = new ISystemWithDesc()
            {
                TimeExtent validTime;
                
                public String getId() { return proc.getId(); }
                public String getUniqueIdentifier() { return proc.getUniqueIdentifier(); }
                public String getName() { return proc.getName(); }
                public String getDescription() { return proc.getDescription(); }
                public Map<QName, Object> getProperties() { return proc.getProperties(); }
                public AbstractProcess getFullDescription() { return proc.getFullDescription(); }  
                
                public TimeExtent getValidTime()
                {
                    if (validTime == null)
                    {
                        var nextKey = featuresIndex.higherKey((MVFeatureParentKey)e.getKey());
                        if (nextKey != null && nextKey.getInternalID() == e.getKey().getInternalID() &&
                            proc.getValidTime() != null && proc.getValidTime().endsNow())
                            validTime = TimeExtent.period(proc.getValidTime().begin(), nextKey.getValidStartTime());
                        else
                            validTime = proc.getValidTime();
                    }
                    
                    return validTime;
                }                  
            };
            
            return new DataUtils.MapEntry<FeatureKey, ISystemWithDesc>(e.getKey(), procWrap);
        });
        
        // apply post filter on time now that we computed the correct valid time period
        if (filter.getValidTime() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        return resultStream;
    }


    @Override
    public void linkTo(IDataStreamStore dataStreamStore)
    {
        this.dataStreamStore = Asserts.checkNotNull(dataStreamStore, IDataStreamStore.class);
    }    

}
