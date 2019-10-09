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
     * @param feature The feature object to be stored
     * @return The key associated with the new feature
     */
    public K add(V feature);
    
    
    /**
     * Add a new version of an existing feature
     * @param feature The feature object to be stored
     * @return The key associated with the new feature
     */
    public K addVersion(V feature);
    
    
    /**
     * Generate a unique key for the specified feature.<br/>
     * Client is responsible for synchronizing on the store to make sure the key
     * is not used by a concurrent call to one of the insertion methods.
     * @param feature
     * @return The unique feature key, including the internal ID
     */
    public K generateKey(V feature);
    
    
    /**
     * Helper method to retrieve the full key corresponding to the latest version
     * of the feature with the given unique ID
     * @param uid The feature unique ID
     * @return The feature key or null if none was found with this UID
     */
    public default K getLatestVersionKey(String uid)
    {
        Entry<K, V> e = getLatestVersionEntry(uid);
        return e != null ? e.getKey() : null;
    }
    
    
    /**
     * Helper method to retrieve the latest version of the feature with the given
     * unique ID
     * @param uid The feature unique ID
     * @return The feature representation or null if none was found with this UID
     */
    public default V getLatestVersion(String uid)
    {
        Entry<K, V> e = getLatestVersionEntry(uid);
        return e != null ? e.getValue() : null;
    }
    
    
    /**
     * Helper method to retrieve the entry corresponding to the latest version
     * of the feature with the given unique ID
     * @param uid The feature unique ID
     * @return The feature entry or null if none was found with this UID
     */
    public default Entry<K, V> getLatestVersionEntry(String uid)
    {
        return selectEntries(FeatureFilter.builder()
                .withUniqueIDs(uid)
                .build())
            .findFirst()
            .orElse(null);            
    }


    /**
     * Checks if store contains a feature with the given unique ID
     * @param uid The feature unique ID
     * @return True if a procedure with the given ID exists, false otherwise
     */
    public default boolean contains(String uid)
    {
        FeatureKey key = FeatureKey.builder().withUniqueID(uid).build();
        return containsKey(key);
    }
    
    
    /**
     * Helper method to remove all versions of the feature with the given UID
     * @param uid The feature unique ID
     * @return The feature key of the last version or null if nothing was removed
     */
    public default FeatureKey remove(String uid)
    {
        FeatureFilter filter = FeatureFilter.builder().withUniqueIDs(uid).build();
        return removeEntries(filter).reduce((k1, k2) -> k2).orElse(null);
    }
       
    
    /**
     * @return Total number of distinct features contained in this data store.<br/>
     * Note that this is different from {@link #getNumRecords()} because the
     * later will count one entry for each version of the same feature while this
     * method will count the feature only once.
     */
    public long getNumFeatures();
    
    
    /**
     * Gets the full feature ID from either the internal ID or the unique ID
     * @param key A key with only one of the two IDs set
     * @return A feature ID object or null if no feature found for the given key
     */
    public FeatureId getFeatureID(K key);
    
    
    /**
     * @return IDs of all features contained in this data store
     */
    public Stream<FeatureId> getAllFeatureIDs();


    /**
     * @return Overall bounding rectangle of all features contained in this
     * data store
     */
    public Bbox getFeaturesBbox();
    
    
    /**
     * Gets a set of more precise bounding rectangles for regions where features
     * contained in this data store are located
     * @param filter Spatial filter to limit the search
     * @return Stream of bounding boxes
     *
    public Stream<Bbox> getFeatureClusters(SpatialFilter filter);*/
    
}
