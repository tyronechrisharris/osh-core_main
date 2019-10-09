/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.ZoneOffset;
import org.h2.mvstore.MVStore;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.procedure.IProcedureDescriptionStore;
import net.opengis.gml.v32.TimePeriod;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Procedure description store implementation based on H2 MVStore.<br/>
 * Most of the work is done in {@link MVBaseFeatureStoreImpl} 
 * </p>
 *
 * @author Alex Robin
 * @date Apr 8, 2018
 */
public class MVProcedureStoreImpl extends MVBaseFeatureStoreImpl<AbstractProcess> implements IProcedureDescriptionStore
{
    MVObsStoreImpl obsStore;
    
    
    protected MVProcedureStoreImpl()
    {
    }
    
    
    /**
     * Opens an existing feature store with the specified name
     * @param mvStore MVStore instance containing the required maps
     * @param dataStoreName name of data store to open
     * @return The existing datastore instance 
     */
    public static MVProcedureStoreImpl open(MVStore mvStore, String dataStoreName)
    {
        MVDataStoreInfo dataStoreInfo = H2Utils.loadDataStoreInfo(mvStore, dataStoreName);
        return (MVProcedureStoreImpl)new MVProcedureStoreImpl().init(mvStore, dataStoreInfo);
    }
    
    
    /**
     * Create a new feature store with the provided info
     * @param mvStore MVStore instance where the maps will be created
     * @param dataStoreInfo new data store info
     * @return The new datastore instance 
     */
    public static MVProcedureStoreImpl create(MVStore mvStore, MVDataStoreInfo dataStoreInfo)
    {
        H2Utils.addDataStoreInfo(mvStore, dataStoreInfo);
        return (MVProcedureStoreImpl)new MVProcedureStoreImpl().init(mvStore, dataStoreInfo);
    }
    
    
    @Override
    public synchronized AbstractProcess put(FeatureKey key, AbstractProcess value)
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();
                        
            try
            {
                var oldValue = super.put(key, value);
                
                // update end of valid time of previous version if any
                var prevEntry = featuresIndex.lowerEntry(key);
                if (prevEntry != null && prevEntry.getKey().getInternalID() == key.getInternalID())
                {
                    var validTimes = prevEntry.getValue().getValidTimeList();
                    if (!validTimes.isEmpty())
                    {
                        var validTime = validTimes.get(0);
                        if (validTime instanceof TimePeriod)
                            ((TimePeriod)validTime).getEndPosition().setDateTimeValue(key.getValidStartTime().atOffset(ZoneOffset.UTC));
                        featuresIndex.put(prevEntry.getKey(), prevEntry.getValue());
                    }
                }
                
                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    /**
     * Link this store to an observation store to enable JOIN queries
     * @param obsStore
     */
    public void linkTo(IObsStore obsStore)
    {
        this.obsStore = (MVObsStoreImpl)obsStore;
    }    

}
