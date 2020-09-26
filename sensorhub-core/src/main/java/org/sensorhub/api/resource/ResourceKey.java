/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.resource;

import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * Key used for indexing and referencing resources by their internal ID
 * </p>
 *
 * @author Alex Robin
 * @date Oct 29, 2018
 */
public class ResourceKey<T extends ResourceKey<T>> implements IResourceKey, Comparable<T>
{
    protected long internalID = -1;
    
    
    /* for deserialization only */
    protected ResourceKey()
    {        
    }
    
    
    public ResourceKey(long internalID)
    {
        Asserts.checkArgument(internalID > 0, "internalID must be > 0");
        this.internalID = internalID;
    }
    
    
    /**
     * @return Resource internal numeric ID
     */
    public long getInternalID()
    {
        return internalID;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
    

    @Override
    public int hashCode()
    {
        return Long.hashCode(getInternalID());
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;
        
        if (!(obj instanceof ResourceKey))
            return false;
        
        ResourceKey<?> other = (ResourceKey<?>)obj;
        return getInternalID() == other.getInternalID();
    }


    @Override
    public int compareTo(T o)
    {
        return Long.compare(internalID, o.getInternalID());
    }
}
