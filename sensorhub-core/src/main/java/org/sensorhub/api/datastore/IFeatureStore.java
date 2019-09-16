/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.stream.Stream;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Bbox;


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
public interface IFeatureStore<K extends FeatureKey, V extends IFeature> extends IDataStore<K, V, IFeatureFilter>
{
    
    /**
     * Add a new feature and generate a new unique key for it
     * @param feature the feature object to be stored
     * @return The key associated with the new feature
     */
    public K add(V feature);
    
    
    /**
     * Helper method to retrieve a feature by its globally unique ID
     * @param uid feature unique ID
     * @return The latest version of the feature or null if none was
     * found with the given ID
     */
    public default V get(String uid)
    {
        FeatureKey key = FeatureKey.builder().withUniqueID(uid).build();
        return get(key);
    }
    
    
    /**
     * Get the data store key associated with the latest version of the
     * feature with the specified UID
     * @param uid feature UID
     * @return the complete key for the latest version of the feature
     */
    public K getLastKey(String uid);


    /**
     * Checks if store contains a feature with the given unique ID
     * @param uid feature unique ID
     * @return True if a procedure with the given ID exists, false otherwise
     */
    public default boolean contains(String uid)
    {
        FeatureKey key = FeatureKey.builder().withUniqueID(uid).build();
        return containsKey(key);
    }
    
    
    /**
     * Helper method to remove all versions of the feature with the given UID
     * @param uid feature unique ID
     * @return true if feature was removed, false otherwise
     */
    public default boolean remove(String uid)
    {
        FeatureFilter filter = FeatureFilter.builder().withUniqueIDs(uid).build();
        return removeEntries(filter).count() > 0;
    }
       
    
    /**
     * @return Total number of distinct features contained in this data store.<br/>
     * Note that this is different from {@link #getNumRecords()} because the
     * later will count one entry for each version of the same feature while this
     * method will count the feature only once.
     */
    public long getNumFeatures();
    
    
    /**
     * @return Unique IDs of all features contained in this data store
     */
    public Stream<String> getAllFeatureUIDs();


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
     *
    public Stream<Bbox> getFeatureClusters(SpatialFilter filter);*/
    
}
