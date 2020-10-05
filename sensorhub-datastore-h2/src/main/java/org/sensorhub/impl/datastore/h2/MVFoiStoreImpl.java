/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.obs.FoiFilter;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.obs.ObsFilter;
import org.sensorhub.api.obs.IFoiStore.FoiField;
import org.sensorhub.api.obs.IObsStore.ObsField;
import org.vast.ogc.gml.IGeoFeature;


/**
 * <p>
 * FOI Store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVFoiStoreImpl extends MVBaseFeatureStoreImpl<IGeoFeature, FoiField, FoiFilter> implements IFoiStore
{
    MVObsStoreImpl obsStore;
    
    
    protected MVFoiStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing feature store with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param dataStoreName name of data store to open
     * @return The existing datastore instance 
     */
    public static MVFoiStoreImpl open(MVStore mvStore, String dataStoreName)
    {
        MVDataStoreInfo dataStoreInfo = H2Utils.loadDataStoreInfo(mvStore, dataStoreName);
        return (MVFoiStoreImpl)new MVFoiStoreImpl().init(mvStore, dataStoreInfo, null);
    }
    
    
    /**
     * Create a new feature store with the provided info
     * @param mvStore MVStore instance where the maps will be created
     * @param dataStoreInfo new data store info
     * @return The new datastore instance 
     */
    public static MVFoiStoreImpl create(MVStore mvStore, MVDataStoreInfo dataStoreInfo)
    {
        H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        return (MVFoiStoreImpl)new MVFoiStoreImpl().init(mvStore, dataStoreInfo, null);
    }
    
    
    @Override
    protected Stream<Entry<FeatureKey, IGeoFeature>> getIndexedStream(FoiFilter filter)
    {
        if (((FoiFilter)filter).getObservationFilter() != null)
        {
            // handle case observation filter
            ObsFilter obsFilter = ((FoiFilter)filter).getObservationFilter();
            Set<Long> foiKeys = obsStore.select(obsFilter, ObsField.FOI_ID)
                .map(obs -> obs.getFoiID().getInternalID())
                .collect(Collectors.toSet());
            
            return super.getIndexedStream(FoiFilter.Builder.from(filter)
                .withInternalIDs(foiKeys)
                .build());
        }
        else
            return super.getIndexedStream(filter);
    }


    @Override
    public void linkTo(IObsStore obsStore)
    {
        this.obsStore = (MVObsStoreImpl)obsStore;        
    }    

}
