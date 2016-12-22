/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2016 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor.swe;

import java.util.HashSet;
import org.sensorhub.api.common.CommandStatus;
import org.sensorhub.api.common.CommandStatus.StatusCode;
import org.sensorhub.api.sensor.SensorException;
import org.sensorhub.impl.sensor.AbstractSensorControl;
import org.vast.util.Asserts;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;


public class SWETransactionalSensorControl extends AbstractSensorControl<SWETransactionalSensor>
{
    final DataComponent cmdDescription;
    HashSet<ITaskingCallback> callbacks = new HashSet<ITaskingCallback>();
    
    
    public SWETransactionalSensorControl(SWETransactionalSensor parentSensor, DataComponent cmdDescription)
    {
        super(cmdDescription.getName(), parentSensor);
        this.cmdDescription = cmdDescription;
        Asserts.checkNotNull(cmdDescription.getName(), "Command must have a name");
    }


    @Override
    public DataComponent getCommandDescription()
    {
        return cmdDescription;
    }


    @Override
    public synchronized CommandStatus execCommand(DataBlock command) throws SensorException
    {
        for (ITaskingCallback callback: callbacks)
            callback.onCommandReceived(command);
        
        CommandStatus cmdStatus = new CommandStatus();
        cmdStatus.status = StatusCode.COMPLETED;
        return cmdStatus;
    }
    
    
    public synchronized void registerCallback(ITaskingCallback callback)
    {
        callbacks.add(callback);
    }
}
