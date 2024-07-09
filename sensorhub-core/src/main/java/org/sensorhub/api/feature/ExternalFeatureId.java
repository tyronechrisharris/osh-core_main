/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2024 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.feature;

import java.util.Objects;
import org.sensorhub.api.utils.OshAsserts;
import org.vast.util.Asserts;
import net.opengis.gml.v32.Reference;


public class ExternalFeatureId extends FeatureId
{
    protected final Reference ref;


    public ExternalFeatureId(Reference ref)
    {
        this.ref = Asserts.checkNotNull(ref, Reference.class);
        OshAsserts.checkValidURI(ref.getHref());
    }
    
    
    public ExternalFeatureId(Reference ref, String uid)
    {
        this.ref = Asserts.checkNotNull(ref, Reference.class);
        OshAsserts.checkValidURI(ref.getHref());
        this.uniqueID = OshAsserts.checkValidUID(uid);
    }
    
    
    /**
     * @return The feature external reference
     */
    public Reference getReference()
    {
        return ref;
    }


    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(
                getReference().getHref(),
                getUniqueID());
    }


    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof ExternalFeatureId))
            return false;

        ExternalFeatureId other = (ExternalFeatureId)obj;
        return Objects.equals(getReference().getHref(), other.getReference().getHref()) &&
               Objects.equals(getUniqueID(), other.getUniqueID());
    }


    @Override
    public String toString()
    {
        return uniqueID + " (" + getReference().getHref() + ")";
    }
}
