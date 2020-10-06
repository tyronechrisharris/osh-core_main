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

import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.procedure.ProcedureId;
import org.vast.util.IResource;
import org.vast.util.TimeExtent;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Interface for IDataStreamStore value objects
 * </p>
 *
 * @author Alex Robin
 * @date Mar 23, 2020
 */
public interface IDataStreamInfo extends IResource
{

    /**
     * @return The identifier of the procedure that generated this data stream
     */
    ProcedureId getProcedureID();


    /**
     * @return The name of the datastream, which corresponds to the name of
     * the procedure output that is/was the source of this data stream
     */
    @Override
    String getName();


    /**
     * @return The version of the output record schema used in this data stream
     */
    int getRecordVersion();


    /**
     * @return The data stream record structure
     */
    DataComponent getRecordStructure();


    /**
     * @return The recommended encoding for the data stream
     */
    DataEncoding getRecordEncoding();
    
    
    /**
     * @return The range of phenomenon times of observations that are part
     * of this datastream.
     */
    TimeExtent getPhenomenonTimeRange();
    
    
    /**
     * @return The range of result times of observations that are part
     * of this datastream.
     */
    TimeExtent getResultTimeRange();
    
    
    /**
     * @return True if this datastream contains observations acquired for a discrete
     * number of result times (e.g. model runs, test campaigns, etc.) 
     */
    boolean hasDiscreteResultTimes();
    
    
    /**
     * @return A map of discrete result times to the phenomenon time range of all
     * observations whose result was produced at each result time, or an empty map if
     * {@link #hasDiscreteResultTimes()} returns true.
     */
    Map<Instant, TimeExtent> getDiscreteResultTimes();

}
