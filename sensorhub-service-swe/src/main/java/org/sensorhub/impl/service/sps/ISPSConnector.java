/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import java.util.concurrent.Flow.Subscriber;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.sensorhub.api.command.ICommandAck;
import org.sensorhub.api.common.SensorHubException;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Interface for all SPS connectors capable of transmitting commands.
 * Implementations can send commands to sensors, process, etc.
 * </p>
 *
 * @author Alex Robin
 * @since Dec 13, 2014
 */
public interface ISPSConnector
{
    
    /**
     * @return the configuration of this connector 
     */
    public SPSConnectorConfig getConfig();
    
    
    /**
     * @param timeRange Time range for which descriptions must be retrieved.
     * If set to null, only the latest description should be retrieved
     * @return list of procedure descriptions for selected time period
     */
    public Stream<AbstractProcess> getProcedureDescriptions(TimeExtent timeRange);
    
    
    /**
     * @return Data component describing the possible tasking parameters
     * for the specified procedure (it will be a DataChoice if several commands
     * are supported by the procedure)
     */
    public DataComponent getTaskingParams();
    
    
    /**
     * Sends the command data through the connector and update the task status
     * accordingly
     * @param task
     * @throws SensorHubException
     */
    public void submitTask(ITask task) throws SensorHubException;
    
    
    /**
     * Configure connector for direct tasking
     * @param taskingParams tasking parameters acceptable for this direct
     * tasking session
     */
    public void startDirectTasking(DataComponent taskingParams);
    
    
    /**
     * Send a direct tasking command. {@link #startDirectTasking(ITask) must be called first} 
     * @param data Command data
     * @param ackCallback Callback to receive ACK messages (can be null)
     */
    public void sendCommand(DataBlock data, Consumer<ICommandAck> ackCallback);
    
    
    /**
     * Subscribe to commands received by this procedure
     * @param taskingParams
     * @param subscriber Subscriber interface that will receive notifications
     */
    public void subscribeToCommands(DataComponent taskingParams, Subscriber<DataBlock> subscriber);
}
