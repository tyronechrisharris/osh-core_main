/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.stream.Stream;
import org.vast.util.Bbox;
import net.opengis.gml.v32.AbstractFeature;


/**
 * <p>
 * Generic interface for all feature stores
 * </p>
 * @param <K> Key type
 * @param <V> Value type 
 * @param <F> Filter type
 *
 * @author Alex Robin
 * @date Mar 19, 2018
 */
public interface IFeatureStore<K extends FeatureKey, V extends AbstractFeature, F extends FeatureFilter> extends IDataStore<K, V, F>
{
    
    /**
     * @return URI prefix to prepend to this data store's feature IDs to obtain
     * a globally unique ID. Can be null if no prefix is used.
     */
    public String getFeatureUriPrefix();
    
    
    /**
     * @return Total number of distinct features contained in this data store.<br/>
     * Note that this is different from {@link #getNumRecords()} because the
     * later will count several entries when the same feature changes over time
     * while this method will count it only once.
     */
    public long getNumFeatures();
    
    
    /**
     * @return IDs of all features contained in this data store
     */
    public Stream<String> getAllFeatureIDs();


    /**
     * @return Overall bounding rectangle of all features contained in this
     * data store
     */
    public Bbox getFeaturesBbox();
    
    
    /**
     * Gets a set of more precise bounding rectangles for regions where features
     * contained in this data store are located
     * @param filter spatial filter to limit the search
     * @return Stream of bounding boxes
     */
    public Stream<Bbox> getFeaturesRegionsBbox(SpatialFilter filter);
    
}
