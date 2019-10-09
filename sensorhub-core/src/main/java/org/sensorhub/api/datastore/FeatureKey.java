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
import org.vast.util.BaseBuilder;
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
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected FeatureKey()
    {        
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
    public static class Builder<B extends Builder<B, T>, T extends FeatureKey> extends BaseBuilder<T>
    {
        protected Builder(T instance)
        {
            super(instance);
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


        public B withValidStartTime(Instant start)
        {
            instance.validStartTime = start;
            return (B)this;
        }
        
        
        public B withLatestValidTime()
        {
            instance.validStartTime = Instant.MAX;
            return (B)this;
        }
        
        
        public T build()
        {
            Asserts.checkArgument(!Strings.isNullOrEmpty(instance.uniqueID) || instance.internalID > 0, "uniqueID or internalID must be set");
            Asserts.checkNotNull(instance.validStartTime, "validStartTime");
            
            T newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
