/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.event;

import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.impl.event.EventSourceInfo;
import org.vast.util.Asserts;

public class EventUtils
{
    public static final String PROCEDURE_MAIN_CHANNEL = "/main";
    public static final String PROCEDURE_OUTPUT_CHANNEL = "/outputs/";
    public static final String PROCEDURE_CONTROL_CHANNEL = "/controls/";
    
    
    private EventUtils() {}
    
    
    public static final String getProcedureSourceID(String procedureUID)
    {
        OshAsserts.checkValidUID(procedureUID);
        return procedureUID + PROCEDURE_MAIN_CHANNEL;
    }
    
    
    public static final String getProcedureOutputSourceID(String procedureUID, String outputName)
    {
        OshAsserts.checkValidUID(procedureUID);
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        return procedureUID + PROCEDURE_OUTPUT_CHANNEL + outputName;
    }
    
    
    public static final String getProcedureCommandTopicID(String procedureUID, String commandInputName)
    {
        OshAsserts.checkValidUID(procedureUID);
        Asserts.checkNotNullOrEmpty(commandInputName, "commandInputName");
        return procedureUID + PROCEDURE_CONTROL_CHANNEL + commandInputName + "/commands";
    }
    
    
    public static final String getProcedureCommandAckTopicID(String procedureUID, String commandInputName)
    {
        OshAsserts.checkValidUID(procedureUID);
        Asserts.checkNotNullOrEmpty(commandInputName, "commandInputName");
        return procedureUID + PROCEDURE_CONTROL_CHANNEL + commandInputName + "/commands/ack";
    }
    
    
    public static final IEventSourceInfo getProcedureEventSourceInfo(String procedureUID)
    {
        return getProcedureEventSourceInfo(null, procedureUID);
    }
    
    
    public static final IEventSourceInfo getProcedureEventSourceInfo(String parentGroupUID, String procedureUID)
    {
        return new EventSourceInfo(
            parentGroupUID != null ? parentGroupUID : procedureUID,
            getProcedureSourceID(procedureUID));
    }
    
    
    public static final IEventSourceInfo getOutputEventSourceInfo(String procedureUID, String outputName)
    {
        return getOutputEventSourceInfo(null, procedureUID, outputName);
    }
    
    
    public static final IEventSourceInfo getOutputEventSourceInfo(String parentGroupUID, String procedureUID, String outputName)
    {
        return new EventSourceInfo(
            parentGroupUID != null ? parentGroupUID : procedureUID,
            getProcedureOutputSourceID(procedureUID, outputName));
    }
}
