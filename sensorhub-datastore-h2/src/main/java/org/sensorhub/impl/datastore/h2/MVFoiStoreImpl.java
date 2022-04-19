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

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.feature.IFoiStore.FoiField;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.feature.FeatureWrapper;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.h2.MVDatabaseConfig.IdProviderType;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;


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
    ISystemDescStore procStore;
    IObsStore obsStore;
        
    
    protected MVFoiStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing foi store or create a new one with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param idScope Internal ID scope (database num)
     * @param idProviderType Type of ID provider to use to generate new IDs
     * @param newStoreInfo Data store info to use if a new store needs to be created
     * @return The existing datastore instance 
     */
    public static MVFoiStoreImpl open(MVStore mvStore, int idScope, IdProviderType idProviderType, MVDataStoreInfo newStoreInfo)
    {
        var dataStoreInfo = H2Utils.getDataStoreInfo(mvStore, newStoreInfo.getName());
        if (dataStoreInfo == null)
        {
            dataStoreInfo = newStoreInfo;
            H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        }
        
        return (MVFoiStoreImpl)new MVFoiStoreImpl().init(mvStore, idScope, idProviderType, dataStoreInfo);
    }
    
    
    @Override
    protected void checkParentFeatureExists(BigId parentID) throws DataStoreException
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
            var parentIDStream = DataStoreUtils.selectSystemIDs(procStore, filter.getParentFilter());
            
            if (resultStream == null)
            {
                resultStream = parentIDStream
                    .flatMap(id -> getParentResultStream(id, filter.getValidTime()));
            }
            else
            {
                var parentIDs = parentIDStream
                    .map(id -> id.getIdAsLong())
                    .collect(Collectors.toSet());
                
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
            var f = e.getValue();
            if (f.getValidTime() != null)
            {
                var foiWrap = new FeatureWrapper(f)
                {
                    TimeExtent validTime;
                    public TimeExtent getValidTime()
                    {
                        if (validTime == null)
                        {
                            var nextKey = featuresIndex.higherKey((MVFeatureParentKey)e.getKey());
                            if (nextKey != null && f.getValidTime() != null && f.getValidTime().endsNow() &&
                                nextKey.getInternalID().getIdAsLong() == e.getKey().getInternalID().getIdAsLong())
                            {
                                validTime = TimeExtent.period(f.getValidTime().begin(), nextKey.getValidStartTime());
                            }
                            else
                                validTime = f.getValidTime();
                        }
                        
                        return validTime;
                    }
                };
                
                return new DataUtils.MapEntry<FeatureKey, IFeature>(e.getKey(), foiWrap);
            }
            
            return e;
        });
        
        // apply post filter on time now that we computed the correct valid time period
        if (filter.getValidTime() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        return resultStream;
    }


    @Override
    public void linkTo(ISystemDescStore procStore)
    {
        this.procStore = Asserts.checkNotNull(procStore, ISystemDescStore.class);
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
