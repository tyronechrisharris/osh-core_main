/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.config.DisplayInfo;
import org.sensorhub.api.data.IDataProducer;


/**
 * <p>
 * Configuration class for SOS data providers obtaining their data in a
 * streaming fashion from data producers and the event bus.
 * </p>
 *
 * @author Alex Robin
 * @since Apr 10, 2020
 */
public class StreamWithHistoryDataProviderConfig extends StreamingDataProviderConfig
{
    public enum DataSource
    {
        STATE,
        STORE,
        AUTO
    }
    
    
    @DisplayInfo(desc="Data source used to obtain latest record values")
    public DataSource latestRecordSource = DataSource.STATE;


    @Override
    public ISOSAsyncDataProvider createProvider(SOSService service, String procUID) throws SensorHubException
    {
        try
        {
            return new StreamWithHistoryDataProvider(service, procUID, this);
        }
        catch (ClassCastException e)
        {
            throw new SensorHubException("Procedure " + procUID + " must be a " + IDataProducer.class.getSimpleName());
        }
    }

}
