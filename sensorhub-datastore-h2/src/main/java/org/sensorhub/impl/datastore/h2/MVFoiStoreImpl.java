/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.h2.mvstore.MVStore;
import org.sensorhub.api.datastore.FoiFilter;
import org.sensorhub.api.datastore.IFoiStore;
import org.sensorhub.api.datastore.IObsStore;
import org.vast.ogc.gml.GenericFeature;


/**
 * <p>
 * FOI Store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVFoiStoreImpl extends MVBaseFeatureStoreImpl<GenericFeature, FoiFilter> implements IFoiStore
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
        MVFeatureStoreInfo dataStoreInfo = (MVFeatureStoreInfo)H2Utils.loadDataStoreInfo(mvStore, dataStoreName);
        return (MVFoiStoreImpl)new MVFoiStoreImpl().init(mvStore, dataStoreInfo);
    }
    
    
    /**
     * Create a new feature store with the provided info
     * @param mvStore MVStore instance where the maps will be created
     * @param dataStoreInfo new data store info
     * @return The new datastore instance 
     */
    public static MVFoiStoreImpl create(MVStore mvStore, MVFeatureStoreInfo dataStoreInfo)
    {
        H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        return (MVFoiStoreImpl)new MVFoiStoreImpl().init(mvStore, dataStoreInfo);
    }


    @Override
    public void linkTo(IObsStore obsStore)
    {
        this.obsStore = (MVObsStoreImpl)obsStore;        
    }    

}
