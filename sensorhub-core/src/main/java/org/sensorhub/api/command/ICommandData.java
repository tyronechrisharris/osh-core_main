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

import java.time.Instant;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Represents command data sent to a control interface
 * </p>
 *
 * @author Alex Robin
 * @date Mar 11, 2021
 */
public interface ICommandData
{

    /**
     * @return Info about command stream that this command is attached to
     */
    ICommandStreamInfo getCommandStream();
    
    
    /**
     * @return ID of sender
     */
    String getSenderID();
    
    
    /**
     * @return Command ref id used to associate with ACK message when
     * several commands are pipelined
     */
    long getCommandRefID();
    
    
    /**
     * @return Time the command was issued
     */
    Instant getIssueTime();

    
    /**
     * @return The command parameters (i.e. command data)
     */
    DataBlock getParams();

}