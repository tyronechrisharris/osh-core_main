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
import org.sensorhub.api.datastore.FeatureFilter;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.FoiFilter;
import org.sensorhub.api.datastore.IFeatureFilter;
import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IFoiStore.FoiField;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.IObsStore.ObsField;
import org.sensorhub.api.datastore.ObsFilter;
import net.opengis.gml.v32.AbstractFeature;


/**
 * <p>
 * FOI Store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVFoiStoreImpl extends MVBaseFeatureStoreImpl<AbstractFeature, FoiField> implements IFoiStore
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
    protected Stream<Entry<FeatureKey, AbstractFeature>> getIndexedStream(IFeatureFilter filter)
    {
        if (filter instanceof FoiFilter && ((FoiFilter)filter).getObservationFilter() != null)
        {
            // handle case observation filter
            ObsFilter obsFilter = ((FoiFilter)filter).getObservationFilter();
            Set<Long> foiKeys = obsStore.select(obsFilter, ObsField.FOI_ID)
                .map(obs -> obs.getFoiID().getInternalID())
                .collect(Collectors.toSet());
            
            return super.getIndexedStream(FeatureFilter.Builder.from(filter)
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
