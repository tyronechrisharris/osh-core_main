/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.feature;

import org.sensorhub.api.datastore.IDataStore;
import org.sensorhub.api.datastore.ValueField;
import org.sensorhub.api.feature.IFeatureStore.FeatureField;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Bbox;


/**
 * <p>
 * Generic interface for all feature stores
 * </p>
 * @param <K> Key type
 * @param <V> Value type
 * @param <VF> Value field type
 * @param <F> Filter type
 *
 * @author Alex Robin
 * @date Mar 19, 2018
 */
public interface IFeatureStore<V extends IFeature, VF extends FeatureField> extends IDataStore<FeatureKey, V, VF, IFeatureFilter>
{
    
    public static class FeatureField extends ValueField
    {
        public static final FeatureField UID = new FeatureField("UID");
        public static final FeatureField NAME = new FeatureField("name");
        public static final FeatureField DESCRIPTION = new FeatureField("description");
        public static final FeatureField GEOMETRY = new FeatureField("geometry");
        public static final FeatureField VALID_TIME = new FeatureField("validTime");
        
        public FeatureField(String name)
        {
            super(name);
        }
    }
    
    
    /**
     * Add a new feature and generate a new unique key for it
     * @param feature The feature object to be stored
     * @return The key associated with the new feature
     */
    public FeatureKey add(V feature);
    
    
    /**
     * Add a new version of an existing feature
     * @param feature The feature object to be stored
     * @return The key associated with the new feature
     */
    public FeatureKey addVersion(V feature);
    
    
    /**
     * Helper method to retrieve the full key corresponding to the latest version
     * of the feature with the given unique ID
     * @param uid The feature unique ID
     * @return The feature key or null if no feature with the given ID was found
     */
    public default FeatureKey getLatestVersionKey(String uid)
    {
        return selectKeys(new FeatureFilter.Builder()
                .withUniqueIDs(uid)
                .withLatestVersion()
                .build())
            .findFirst()
            .orElse(null);
    }
    
    
    /**
     * Helper method to retrieve the full key corresponding to the latest version
     * of the feature with the given internal ID
     * @param internalID The feature internal ID
     * @return The feature key or null if no feature with the given ID was found
     */
    public default FeatureKey getLatestVersionKey(long internalID)
    {
        return selectKeys(new FeatureFilter.Builder()
                .withInternalIDs(internalID)
                .withLatestVersion()
                .build())
            .findFirst()
            .orElse(null);
    }
    
    
    /**
     * Helper method to retrieve the entry corresponding to the latest version
     * of the feature with the given unique ID
     * @param uid The feature unique ID
     * @return The feature entry or null if no feature with the given ID was found
     */
    public default Entry<FeatureKey, V> getLatestVersionEntry(String uid)
    {
        return selectEntries(new FeatureFilter.Builder()
                .withUniqueIDs(uid)
                .withLatestVersion()
                .build())
            .findFirst()
            .orElse(null);            
    }
    
    
    /**
     * Helper method to retrieve the entry corresponding to the latest version
     * of the feature with the given internal ID
     * @param internalID The feature internal ID
     * @return The feature entry or null if no feature with the given ID was found
     */
    public default Entry<FeatureKey, V> getLatestVersionEntry(long internalID)
    {
        return selectEntries(new FeatureFilter.Builder()
                .withInternalIDs(internalID)
                .withLatestVersion()
                .build())
            .findFirst()
            .orElse(null);            
    }
    
    
    /**
     * Helper method to retrieve the latest version of the feature with the given
     * unique ID
     * @param uid The feature unique ID
     * @return The feature representation or null if no feature with the
     * given ID was found
     */
    public default V getLatestVersion(String uid)
    {
        Entry<FeatureKey, V> e = getLatestVersionEntry(uid);
        return e != null ? e.getValue() : null;
    }
    
    
    /**
     * Helper method to retrieve the latest version of the feature with the given
     * internal ID
     * @param internalID The feature internal ID
     * @return The feature representation or null if no feature with the
     * given ID was found
     */
    public default V getLatestVersion(long internalID)
    {
        Entry<FeatureKey, V> e = getLatestVersionEntry(internalID);
        return e != null ? e.getValue() : null;
    }


    /**
     * Checks if store contains a feature with the given unique ID
     * @param uid The feature unique ID
     * @return True if a procedure with the given ID exists, false otherwise
     */
    public default boolean contains(String uid)
    {
        return getLatestVersionKey(uid) != null;
    }
    
    
    /**
     * Helper method to remove all versions of the feature with the given UID
     * @param uid The feature unique ID
     * @return The feature key of the last version or null if nothing was removed
     */
    public default FeatureKey remove(String uid)
    {
        FeatureFilter filter = new FeatureFilter.Builder().withUniqueIDs(uid).build();
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
