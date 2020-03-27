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

import java.util.Objects;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * Container class for both internal ID and unique ID (URI) of a feature.
 * A uniqueID can be added after creation but cannot be changed.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 17, 2019
 */
public class FeatureId
{
    public static FeatureId NULL_FEATURE = new FeatureId(0L);
    
    protected long internalID = -1; // 0 is reserved and can never be used as ID
    protected String uniqueID;
    
    
    protected FeatureId()
    {        
    }
    
    
    private FeatureId(long internalID)
    {
        this.internalID = internalID;
    }
    
    
    public FeatureId(long internalID, String uid)
    {
        this(internalID);
        this.uniqueID = Asserts.checkNotNull(uid, "uid");
    }


    /**
     * @return The feature internal ID
     */
    public long getInternalID()
    {
        return internalID;
    }


    /**
     * @return The feature unique ID
     */
    public String getUniqueID()
    {
        return uniqueID;
    }
    
    
    public void setUniqueID(String uid)
    {
        Asserts.checkState(this.uniqueID != null, "Unique ID was already set");
        this.uniqueID = uid;
    }
    

    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(
                getInternalID(),
                getUniqueID());
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof FeatureId))
            return false;
        
        FeatureId other = (FeatureId)obj;
        return getInternalID() == other.getInternalID() &&
               Objects.equals(getUniqueID(), other.getUniqueID());
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
}
