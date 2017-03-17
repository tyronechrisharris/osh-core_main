/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.persistence;

import java.util.Collection;
import org.sensorhub.api.persistence.IFeatureFilter;
import com.vividsolutions.jts.geom.Geometry;
import net.opengis.gml.v32.AbstractFeature;


/**
 * <p>
 * Helper methods to filter SWE objects read from database
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Mar 15, 2017
 */
public class FilterUtils
{
    
    public static boolean isFeatureSelected(IFeatureFilter filter, AbstractFeature f)
    {
        if (filter == null)
            return true;
        
        // feature id criteria
        Collection<String> fids = filter.getFeatureIDs();
        if (fids != null && !fids.isEmpty() && !fids.contains(f.getUniqueIdentifier()))
            return false;
        
        // roi criteria
        if (filter.getRoi() != null)
        {
            Geometry geom = (Geometry)f.getLocation();
            if (geom != null && !filter.getRoi().intersects(geom))
                return false;
        }
        
        return true;        
    }
}
