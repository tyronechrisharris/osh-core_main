/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Proxy used by services generating virtual sensors (e.g. SOS-T, SensorThings)
 * </p>
 *
 * @author Alex Robin
 * @date Sep 10, 2019
 */
public class VirtualSensorProxy extends DataProducerProxy
{
    private static final long serialVersionUID = -3281124788006180015L;

    
    public VirtualSensorProxy(AbstractProcess desc)
    {
        this.lastDescription = desc;
        this.lastDescriptionUpdate = System.currentTimeMillis();
    }
    
    
    public void updateDescription(AbstractProcess desc)
    {
        long now = this.lastDescriptionUpdate = System.currentTimeMillis();
        this.lastDescription = desc;
        updateInDatastore(true);
        eventPublisher.publish(new ProcedureChangedEvent(now, desc.getUniqueIdentifier()));
    }
    
    
    public void newDataRecord(String outputName, DataBlock data)
    {
        OutputProxy output = outputs.get(outputName);
        long now = output.latestRecordTime = System.currentTimeMillis();
        output.latestRecord = data;
        output.eventPublisher.publish(new DataEvent(now, getUniqueIdentifier(), outputName, data));
    }
    
    
    public void delete()
    {
        registry.unregister(this);
    }
}
