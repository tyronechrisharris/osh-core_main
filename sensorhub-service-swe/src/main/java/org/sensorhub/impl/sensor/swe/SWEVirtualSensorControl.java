/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor.swe;

import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.sensorhub.api.command.CommandAck;
import org.sensorhub.api.command.ICommandAck;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.impl.sensor.AbstractSensorControl;
import org.vast.data.AbstractDataBlock;
import org.vast.data.DataBlockInt;
import org.vast.data.DataBlockMixed;
import org.vast.ows.sps.StatusReport.RequestStatus;
import org.vast.util.Asserts;


public class SWEVirtualSensorControl extends AbstractSensorControl<SWEVirtualSensor>
{
    DataComponent cmdDescription;
    DataBlockMixed cmdWrapper;
    
    
    public SWEVirtualSensorControl(SWEVirtualSensor parentSensor, DataComponent cmdDescription)
    {
        this(parentSensor, cmdDescription, -1);
        Asserts.checkNotNull(cmdDescription.getName(), "Command must have a name");
    }
    
    
    public SWEVirtualSensorControl(SWEVirtualSensor parentSensor, DataComponent cmdDescription, int choiceIndex)
    {
        super(parentSensor);
        this.cmdDescription = cmdDescription;
        
        if (choiceIndex >= 0)
        {
            cmdWrapper = new DataBlockMixed(2);
            DataBlockInt choiceIndexData = new DataBlockInt(1);
            choiceIndexData.setIntValue(choiceIndex);
            cmdWrapper.getUnderlyingObject()[0] = choiceIndexData;
        }
    }
    

    @Override
    public String getName()
    {
        return cmdDescription.getName();
    }
    

    @Override
    public DataComponent getCommandDescription()
    {
        return cmdDescription;
    }
    
    
    @Override
    public CompletableFuture<Void> executeCommand(ICommandData command, Consumer<ICommandAck> callback)
    {
        // wrap to add choice index if several commands were advertised by server
        DataBlock commandParams;
        if (cmdWrapper != null)
        {
            cmdWrapper.getUnderlyingObject()[1] = (AbstractDataBlock)command.getParams();
            commandParams = cmdWrapper;
        }
        else
            commandParams = command.getParams();
        
        // execute command asynchronously
        return CompletableFuture.runAsync(() -> {
            try
            {
                // TODO handle asynchronous tasking
                var resp = parentSensor.spsClient.sendTaskMessage(commandParams);
                if (resp.getReport().getRequestStatus() == RequestStatus.Rejected)
                    callback.accept(CommandAck.fail(command.getCommandRefID()));
                else
                    callback.accept(CommandAck.success(command.getCommandRefID()));
            }
            catch (SensorHubException e)
            {
                throw new CompletionException("Error sending command to SPS", e);
            }
        });
    }
    

    @Override
    public void validateCommand(ICommandData command)
    {        
    }
}
