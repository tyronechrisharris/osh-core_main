/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.sensor;

import org.sensorhub.api.data.IStreamingDataInterface;


/**
 * <p>
 * Interface to be implemented by all sensor drivers connected to the system.
 * <p>Data provided by this interface can be actual measurements but also status
 * information. Each sensor output is mapped to a separate instance of this
 * interface, allowing them to have completely independent sampling rates.</p>
 * <p>Implementation MUST send events of type 
 * {@link org.sensorhub.api.sensor.SensorDataEvent} when new data is produced
 * by sensor.</p>
 * <p>2019-02: Removed methods to access stored data. Storage API can be
 * implemented instead if needed.
 * </p>
 * 
 * @author Alex Robin
 * @since Nov 5, 2010
 */
public interface ISensorDataInterface extends IStreamingDataInterface
{

}
