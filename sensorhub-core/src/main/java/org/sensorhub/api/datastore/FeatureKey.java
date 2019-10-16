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
 * Immutable key object used to index features in storage.<br/>
 * The key can include an internal ID or a unique ID or both. If both are
 * set when the key is used for retrieval, only the internal ID is used.
 * </p>
 *
 * @author Alex Robin
 * @since Mar 19, 2018
 */
public class FeatureKey extends FeatureId
{    
    protected Instant validStartTime = Instant.MIN; // use Instant.MAX to retrieve latest version
    
    
    protected FeatureKey()
    {        
    }
    
    
    public FeatureKey(long internalID)
    {
        Asserts.checkArgument(internalID > 0);
        this.internalID = internalID;
    }
    
    
    public FeatureKey(long internalID, Instant validStartTime)
    {
        this(internalID);
        this.validStartTime = Asserts.checkNotNull(validStartTime);
    }
    
    
    public FeatureKey(String uniqueID)
    {
        Asserts.checkArgument(!Strings.isNullOrEmpty(uniqueID));
        this.uniqueID = uniqueID;
    }
    
    
    public FeatureKey(String uniqueID, Instant validStartTime)
    {
        this(uniqueID);
        this.validStartTime = Asserts.checkNotNull(validStartTime);
    }
    
    
    public FeatureKey(long internalID, String uniqueID, Instant validStartTime)
    {
        this(internalID);
        this.uniqueID = uniqueID;
        this.validStartTime = Asserts.checkNotNull(validStartTime);
    }
    
    
    /**
     * @param internalID
     * @return A feature key with given internal ID and the latest valid time
     */
    public static FeatureKey latest(long internalID)
    {
        return new FeatureKey(internalID, Instant.MAX);
    }


    /**
     * @return The time range of validity of the feature description.<br/>
     * The time range must end with {@link Double.POSITIVE_INFINITY} if the end of validity is not yet known.<br/>
     * If null, the feature will always be returned by queries containing a temporal filter 
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
