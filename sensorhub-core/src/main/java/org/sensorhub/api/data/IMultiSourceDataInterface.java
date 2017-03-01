/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.data;

import java.util.Collection;
import java.util.Map;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Data interface used with {@link org.sensorhub.api.data.IMultiSourceDataProducer}.<br/>
 * This adds support for multiplexing multiple data streams in a single output
 * by specifying the ID of the source entity in the data record.<br/>
 * The data component carrying the entity ID MUST be tagged with the role
 * {@link #ENTITY_ID_URI}
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Feb 19, 2017
 */
public interface IMultiSourceDataInterface extends IStreamingDataInterface
{
    public final static String ENTITY_ID_URI = "urn:osh:entityID";
    
    
    /**
     * @return List of entity IDs for which this output produces data
     */
    public Collection<String> getEntityIDs();
    
    
    /**
     * Gets the latest records received on this data channel from any entity.
     * @return the last measurement records or an empty map if no data is available
     */
    public Map<String, DataBlock> getLatestRecords();
    
    
    /**
     * Gets the latest record received on this data channel from the
     * specified entity.
     * @return the last measurement record or null if no data is available
     */
    public DataBlock getLatestRecord(String entityID);
}
