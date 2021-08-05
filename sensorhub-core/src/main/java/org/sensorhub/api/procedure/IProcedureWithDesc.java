/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.vast.ogc.om.IProcedure;
import org.vast.swe.SWEConstants;
import net.opengis.sensorml.v20.AbstractPhysicalProcess;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.PhysicalComponent;
import net.opengis.sensorml.v20.PhysicalSystem;


/**
 * <p>
 * Interface for procedure resources associated to a SensorML description.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public interface IProcedureWithDesc extends IProcedure
{

    @Override
    public default String getType()
    {
        var sml = getFullDescription();
        
        // use definition or generate default type
        if (sml.getDefinition() != null)
        {
            return sml.getDefinition();
        }
        else if (sml instanceof AbstractPhysicalProcess)
        {
            if (sml instanceof PhysicalComponent ||
               (sml instanceof PhysicalSystem && ((PhysicalSystem)sml).getNumComponents() == 0))
            {
                if (sml.getNumOutputs() > 0)
                    return SWEConstants.DEF_SENSOR;
                else if (sml.getNumInputs() > 0)
                    return SWEConstants.DEF_ACTUATOR;
            }
            
            return SWEConstants.DEF_SYSTEM;
        }
        else if (sml instanceof AbstractProcess)
        {
            return SWEConstants.DEF_PROCESS;
        }
        else
            return SWEConstants.NIL_UNKNOWN;
    }
    
    
    AbstractProcess getFullDescription();
    
}
