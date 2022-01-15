/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.command;

import org.sensorhub.api.system.SystemId;
import org.vast.util.IResource;
import org.vast.util.TimeExtent;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Interface for command stream descriptors
 * </p>
 *
 * @author Alex Robin
 * @date Mar 26, 2021
 */
public interface ICommandStreamInfo extends IResource
{
    
    /**
     * @return The identifier of the system that exposes this command stream
     */
    SystemId getSystemID();

    
    /**
     * @return The name of the system control input that receives commands
     * from this stream
     */
    String getControlInputName();
    

    /**
     * @return The structure of the command parameters
     */
    DataComponent getRecordStructure();

    
    /**
     * @return The recommended encoding for the command parameters
     */
    DataEncoding getRecordEncoding();
    
    
    /**
     * @return The time of validity of this command stream. This corresponds
     * to the time during which the corresponding control input actually
     * existed.
     */
    TimeExtent getValidTime();
    
    
    /**
     * @return The range of issue times of commands that are part of this
     * command stream, or null if no commands have been recorded yet.
     */
    TimeExtent getIssueTimeRange();
    
    
    /**
     * @return The range of actuation times of commands that are part of this
     * command stream, or null if no commands have been recorded yet.
     */
    TimeExtent getExecutionTimeRange();

}
