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
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.feature.IFoiStore.FoiField;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.h2.MVDatabaseConfig.IdProviderType;
import org.sensorhub.impl.datastore.h2.MVFeatureStoreImpl.IGeoTemporalFeature;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.hash.Hashing;
import net.opengis.gml.v32.AbstractGeometry;


/**
 * <p>
 * FOI Store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVFoiStoreImpl extends MVBaseFeatureStoreImpl<IFeature, FoiField, FoiFilter> implements IFoiStore
{
    IProcedureStore procStore;
    IObsStore obsStore;
    
    
    protected MVFoiStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing foi store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param idProviderType Type of ID provider to use to generate new IDs
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVFoiStoreImpl open(MVStore mvStore, IdProviderType idProviderType, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        // create ID provider
        IdProvider<IFeature> idProvider = null;
        if (idProviderType == IdProviderType.UID_HASH)
        {
            var hashFunc = Hashing.murmur3_128(842156962);
            idProvider = f -> {
                var hc = hashFunc.hashUnencodedChars(f.getUniqueIdentifier());
                return hc.asLong() & 0xFFFFFFFFFFFFL; // keep only 48 bits
            };
        }
        
        return (MVFoiStoreImpl)new MVFoiStoreImpl().init(mvStore, dataStoreInfo, idProvider);
    }
    
    
    @Override
    protected void checkParentFeatureExists(long parentID) throws DataStoreException
    {
        if (procStore != null)
            DataStoreUtils.checkParentFeatureExists(parentID, procStore, this);
        else
            DataStoreUtils.checkParentFeatureExists(parentID, this);
    }
    
    
    @Override
    protected Stream<Entry<MVFeatureParentKey, IFeature>> getIndexedStream(FoiFilter filter)
    {
        var resultStream = super.getIndexedStream(filter);
        
        if (filter.getParentFilter() != null)
        {
            var parentIDStream = DataStoreUtils.selectProcedureIDs(procStore, filter.getParentFilter());
            
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
        
        if (filter.getObservationFilter() != null)
        {
            var foiIDs = obsStore.selectObservedFois(filter.getObservationFilter())
                .collect(Collectors.toSet());
            
            if (resultStream == null)
            {            
                resultStream = super.getIndexedStream(FoiFilter.Builder.from(filter)
                    .withInternalIDs(foiIDs)
                    .build());
            }
            else
            {
                resultStream = resultStream
                    .filter(e -> foiIDs.contains(e.getKey().getInternalID()));
            }
        }
        
        return resultStream;
    }


    @Override
    public Stream<Entry<FeatureKey, IFeature>> selectEntries(FoiFilter filter, Set<FoiField> fields)
    {
        // update validTime in the case it ends at now and there is a
        // more recent version of the feature description available
        Stream<Entry<FeatureKey, IFeature>> resultStream = super.selectEntries(filter, fields).map(e -> {
            if (e.getValue() instanceof IGeoTemporalFeature)
            {
                var f = (IGeoTemporalFeature)e.getValue();
                var procWrap = new IGeoTemporalFeature()
                {
                    TimeExtent validTime;
                    
                    public String getId() { return f.getId(); }
                    public String getUniqueIdentifier() { return f.getUniqueIdentifier(); }
                    public String getName() { return f.getName(); }
                    public String getDescription() { return f.getDescription(); }
                    public AbstractGeometry getGeometry() { return f.getGeometry(); }
                    public Map<QName, Object> getProperties() { return f.getProperties(); } 
                    
                    public TimeExtent getValidTime()
                    {
                        if (validTime == null)
                        {
                            var nextKey = featuresIndex.higherKey((MVFeatureParentKey)e.getKey());
                            if (nextKey != null && nextKey.getInternalID() == e.getKey().getInternalID() &&
                                f.getValidTime() != null && f.getValidTime().endsNow())
                                validTime = TimeExtent.period(f.getValidTime().begin(), nextKey.getValidStartTime());
                            else
                                validTime = f.getValidTime();
                        }
                        
                        return validTime;
                    }
                };
                
                return new DataUtils.MapEntry<FeatureKey, IFeature>(e.getKey(), procWrap);
            }
            
            return e;
        });
        
        // apply post filter on time now that we computed the correct valid time period
        if (filter.getValidTime() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        return resultStream;
    }
    
    
    protected Stream<Long> selectParentIDs(ProcedureFilter parentFilter)
    {
        return DataStoreUtils.selectFeatureIDs(procStore, parentFilter);
    }


    @Override
    public void linkTo(IProcedureStore procStore)
    {
        this.procStore = Asserts.checkNotNull(procStore, IProcedureStore.class);
    }


    @Override
    public void linkTo(IObsStore obsStore)
    {
        this.obsStore = Asserts.checkNotNull(obsStore, IObsStore.class);
    }
    
    
    @Override
    public void linkTo(IFeatureStore featureStore)
    {
        throw new UnsupportedOperationException();
    }

}
