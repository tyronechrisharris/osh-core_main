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
public class FeatureKey
{    
    protected long internalID = -1; // 0 is reserved and can never be used as ID
    protected String uniqueID;
    protected Instant validStartTime = Instant.MIN; // use Instant.MAX to retrieve latest version
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected FeatureKey()
    {        
    }


    public long getInternalID()
    {
        return internalID;
    }


    /**
     * @return The Unique ID of feature.<br/>
     * This cannot be null.
     */
    public String getUniqueID()
    {
        return uniqueID;
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
    
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T extends Builder> T builder()
    {
        return (T)new Builder(new FeatureKey());
    }
    
    
    @SuppressWarnings("unchecked")
    public static class Builder<B extends Builder<B, T>, T extends FeatureKey> implements IBuilder<T>
    {
        protected T instance;


        protected Builder(T instance)
        {
            this.instance = instance;
        }


        public B withInternalID(long internalID)
        {
            instance.internalID = internalID;
            return (B)this;
        }


        public B withUniqueID(String uniqueID)
        {
            instance.uniqueID = uniqueID;
            return (B)this;
        }


        /**
         * @param start 
         * @return builder for chaining
         */
        public B withValidStartTime(Instant start)
        {
            instance.validStartTime = start;
            return (B)this;
        }
        
        
        public T build()
        {
            Asserts.checkArgument(instance.uniqueID != null || instance.internalID > 0, "UniqueID or internalID must be set");
            Asserts.checkNotNull(instance.validStartTime, "validStartTime");
            
            T newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
