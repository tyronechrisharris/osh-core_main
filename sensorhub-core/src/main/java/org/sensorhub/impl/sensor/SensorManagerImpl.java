/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.sensor.ISensor;
import org.sensorhub.api.sensor.ISensorManager;


/**
 * <p>
 * Default implementation of the sensor manager interface
 * </p>
 *
 * @author Alex Robin
 * @since Sep 7, 2013
 */
public class SensorManagerImpl implements ISensorManager
{
    protected IProcedureRegistry entityManager;
    
    
    public SensorManagerImpl(ISensorHub hub)
    {
        this.entityManager = hub.getProcedureRegistry();
    }


    @Override
    public Collection<ISensor> getConnectedSensors(int maxLevels)
    {
        ArrayList<ISensor> connectedSensors = new ArrayList<>();
        
        // delegate to entity manager and filter on connected status
        for (ISensor sensor: entityManager.list(ISensor.class))
        {
            if (sensor.isConnected())
                connectedSensors.add(sensor);
        }
        
        return connectedSensors;
    }


    @Override
    public Collection<ISensor> findSensors(Predicate<ISensor> filter)
    {
        // TODO Auto-generated method stub
        return null;
    }

}
