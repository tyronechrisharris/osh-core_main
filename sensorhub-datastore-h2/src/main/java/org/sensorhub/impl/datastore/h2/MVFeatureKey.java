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

import java.time.Instant;
import java.util.Objects;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;
import com.google.common.base.Strings;


/**
 * <p>
 * Immutable key object used to index features in data stores.<br/>
 * The key can include an internal ID or a unique ID or both. If both are
 * set when the key is used for retrieval, only the internal ID is used.
 * </p>
 *
 * @author Alex Robin
 * @since Mar 19, 2018
 */
public class FeatureKey extends FeatureId
{    
    public final static Instant TIMELESS = Instant.MIN;
    public final static Instant LATEST = Instant.MAX;
    
    protected Instant validStartTime = TIMELESS;
    
    
    protected FeatureKey()
    {        
    }
    
    
    /**
     * Creates an insertion key for a timeless feature
     * @param internalID Feature internal ID
     * @param uniqueID Feature unique ID
     */
    public FeatureKey(long internalID, String uniqueID)
    {
        this(internalID);
        this.uniqueID = uniqueID;
    }
    
    
    /**
     * Creates an insertion key for a feature with a limited time validity
     * @param internalID Feature internal ID
     * @param uniqueID Feature unique ID
     * @param validStartTime Start of feature validity period
     */
    public FeatureKey(long internalID, String uniqueID, Instant validStartTime)
    {
        this(internalID, uniqueID);
        this.validStartTime = Asserts.checkNotNull(validStartTime);
    }
    
    
    /**
     * Creates a retrieval key for a timeless feature
     * @param internalID Internal ID of desired feature
     */
    public FeatureKey(long internalID)
    {
        Asserts.checkArgument(internalID > 0, "internalID must be > 0");
        this.internalID = internalID;
    }
    
    
    /**
     * Creates a retrieval key for a timeless feature
     * @param uniqueID Unique ID of desired feature
     */
    public FeatureKey(String uniqueID)
    {
        Asserts.checkArgument(!Strings.isNullOrEmpty(uniqueID), "uniqueID cannot be null or empty");
        this.uniqueID = uniqueID;
    }
    
    
    /**
     * Creates a retrieval key for the version of a feature valid at a specific time
     * @param internalID Internal ID of desired feature
     * @param validStartTime Start of feature version validity period
     */
    public FeatureKey(long internalID, Instant validStartTime)
    {
        this(internalID);
        this.validStartTime = Asserts.checkNotNull(validStartTime);
    }
    
    
    /**
     * Creates a retrieval key for the version of a feature valid at a specific time
     * @param uniqueID Unique ID of desired feature
     * @param validStartTime Start of feature version validity period
     */
    public FeatureKey(String uniqueID, Instant validStartTime)
    {
        this(uniqueID);
        this.validStartTime = Asserts.checkNotNull(validStartTime);
    }
    
    
    /**
     * Creates a retrieval key for the latest version of a feature
     * @param internalID Feature internal ID
     * @return The feature key object
     */
    public static FeatureKey latest(long internalID)
    {
        return new FeatureKey(internalID, Instant.MAX);
    }
    
    
    /**
     * Creates a retrieval key for the latest version of a feature
     * @param uniqueID Unique ID of desired feature
     * @return The feature key object
     */
    public static FeatureKey latest(String uniqueID)
    {
        return new FeatureKey(uniqueID, Instant.MAX);
    }


    /**
     * @return The start of validity of the feature description.<br/>
     * If null or equal to {@link TIMELESS}, the feature will always be returned
     * by queries containing a temporal filter 
     */    
    public Instant getValidStartTime()
    {
        return validStartTime;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
    

    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(
                getInternalID(),
                getUniqueID(),
                getValidStartTime());
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof FeatureKey))
            return false;
        
        FeatureKey other = (FeatureKey)obj;
        return getInternalID() == other.getInternalID() &&
               Objects.equals(getUniqueID(), other.getUniqueID()) &&
               Objects.equals(getValidStartTime(), other.getValidStartTime());
    }
}
