/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import java.util.Map;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.feature.IFoiStore.FoiField;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.impl.database.registry.FederatedObsDatabase.LocalFilterInfo;
import org.vast.ogc.gml.IGeoFeature;


/**
 * <p>
 * Implementation of foi store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 3, 2019
 */
public class FederatedFoiStore extends FederatedBaseFeatureStore<IGeoFeature, FoiField, FoiFilter> implements IFoiStore
{
        
    FederatedFoiStore(IDatabaseRegistry registry, FederatedObsDatabase db)
    {
        super(registry, db);
    }
    
    
    protected IFoiStore getFeatureStore(IProcedureObsDatabase db)
    {
        return db.getFoiStore();
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(FoiFilter filter)
    {
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = parentDb.getFilterDispatchMap(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = FoiFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getParentFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = parentDb.procStore.getFilterDispatchMap(filter.getParentFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = FoiFilter.Builder
                        .from(filter)
                        .withParents((ProcedureFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getObservationFilter() != null)
        {
            // delegate to proc store handle procedure filter dispatch map
            var filterDispatchMap = parentDb.obsStore.getFilterDispatchMap(filter.getObservationFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = FoiFilter.Builder
                        .from(filter)
                        .withObservations((ObsFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }


    @Override
    public void linkTo(IProcedureStore procStore)
    {
        throw new UnsupportedOperationException();        
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void linkTo(IFeatureStore featureStore)
    {
        throw new UnsupportedOperationException();
    }

}
