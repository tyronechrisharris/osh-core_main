/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import org.vast.ogc.gml.GenericFeature;


/**
 * <p>
 * Generic interface for all feature of interest stores
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2018
 */
public interface IFoiStore extends IFeatureStore<FeatureKey, GenericFeature, FoiFilter>
{
    
    /**
     * Link this store to an observation store to enable JOIN queries
     * @param obsStore
     */
    public void linkTo(IObsStore obsStore);
}
