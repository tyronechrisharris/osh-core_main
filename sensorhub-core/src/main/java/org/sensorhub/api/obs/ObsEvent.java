/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.obs;

import java.util.Arrays;
import java.util.Collection;
import org.vast.util.Asserts;


/**
 * <p>
 * Event carrying one or more observations by reference
 * </p>
 *
 * @author Alex Robin
 * @date Mar 10, 2021
 */
public class ObsEvent extends DataStreamEvent
{
    Collection<IObsData> observations;
    
    
    public ObsEvent(long timeStamp, String procUID, String outputName, IObsData... obs)
    {
        super(timeStamp, procUID, outputName);
        this.observations = Asserts.checkNotNullOrEmpty(Arrays.asList(obs), IObsData[].class);
    }


    public Collection<IObsData> getObservations()
    {
        return observations;
    }    
    
}
