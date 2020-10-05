/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.h2.mvstore.MVStore;
import org.sensorhub.api.feature.FeatureFilter;
import org.sensorhub.api.feature.IFeatureStore;
import org.sensorhub.api.feature.IFeatureStoreBase.FeatureField;
import org.vast.ogc.gml.IGeoFeature;


public class MVFeatureStoreImpl extends MVBaseFeatureStoreImpl<IGeoFeature, FeatureField, FeatureFilter> implements IFeatureStore
{

    protected MVFeatureStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing feature store with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param dataStoreName name of data store to open
     * @return The existing datastore instance 
     */
    public static MVFeatureStoreImpl open(MVStore mvStore, String dataStoreName)
    {
        MVDataStoreInfo dataStoreInfo = H2Utils.loadDataStoreInfo(mvStore, dataStoreName);
        return (MVFeatureStoreImpl)new MVFeatureStoreImpl().init(mvStore, dataStoreInfo, null);
    }
    
    
    /**
     * Create a new feature store with the provided info
     * @param mvStore MVStore instance where the maps will be created
     * @param dataStoreInfo new data store info
     * @return The new datastore instance 
     */
    public static MVFeatureStoreImpl create(MVStore mvStore, MVDataStoreInfo dataStoreInfo)
    {
        H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        return (MVFeatureStoreImpl)new MVFeatureStoreImpl().init(mvStore, dataStoreInfo, null);
    }
    

}
