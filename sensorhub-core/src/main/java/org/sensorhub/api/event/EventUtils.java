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

import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.command.IStreamingControlInterface;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.procedure.IProcedureDriver;
import org.sensorhub.api.utils.OshAsserts;
import org.vast.util.Asserts;


public class EventUtils
{
    public static final String MODULE_TOPIC_PREFIX = "modules/";
    public static final String PROCEDURE_TOPIC_PREFIX = "procedures/";
    public static final String PROCEDURE_OUTPUT_CHANNELS = "/outputs/";
    public static final String PROCEDURE_CONTROL_CHANNELS = "/commands/";
    public static final String PROCEDURE_TASKS_CHANNELS = "/tasks/";
    public static final String STATUS_CHANNEL = "/status";
    public static final String DATA_CHANNEL = "/data";
    public static final String ACK_CHANNEL = "/ack";
    
    
    private EventUtils() {}
    
    
    public static final String getModuleRegistryTopicID()
    {
        return MODULE_TOPIC_PREFIX;
    }
    
    
    public static final String getModuleTopicID(String moduleID)
    {
        OshAsserts.checkValidUID(moduleID, "ModuleID");
        return MODULE_TOPIC_PREFIX + moduleID;
    }
    
    
    public static final String getProcedureRegistryTopicID()
    {
        return PROCEDURE_TOPIC_PREFIX;
    }
    
    
    public static final String getProcedurePublisherGroupID(String procedureUID)
    {
        OshAsserts.checkValidUID(procedureUID);
        return PROCEDURE_TOPIC_PREFIX + procedureUID;
    }
    
    
    public static final String getProcedureStatusTopicID(String procedureUID)
    {
        OshAsserts.checkValidUID(procedureUID);
        return PROCEDURE_TOPIC_PREFIX + procedureUID + STATUS_CHANNEL;
    }
    
    
    public static final String getProcedureStatusTopicID(IProcedureDriver driver)
    {
        return getProcedureStatusTopicID(driver.getUniqueIdentifier());
    }
    
    
    static final String getDataStreamTopicID(String procedureUID, String outputName)
    {
        OshAsserts.checkValidUID(procedureUID);
        Asserts.checkNotNullOrEmpty(outputName, "outputName");
        return PROCEDURE_TOPIC_PREFIX + procedureUID + PROCEDURE_OUTPUT_CHANNELS + outputName;
    }
    
    
    public static final String getDataStreamStatusTopicID(String procedureUID, String outputName)
    {
        return getDataStreamTopicID(procedureUID, outputName) + STATUS_CHANNEL;
    }
    
    
    public static final String getDataStreamStatusTopicID(IStreamingDataInterface outputInterface)
    {
        return getDataStreamStatusTopicID(
            outputInterface.getParentProducer().getUniqueIdentifier(),
            outputInterface.getName());
    }
    
    
    public static final String getDataStreamStatusTopicID(IDataStreamInfo dsInfo)
    {
        return getDataStreamStatusTopicID(
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName());
    }
    
    
    public static final String getDataStreamDataTopicID(String procedureUID, String outputName)
    {
        return getDataStreamTopicID(procedureUID, outputName) + DATA_CHANNEL;
    }
    
    
    public static final String getDataStreamDataTopicID(IStreamingDataInterface outputInterface)
    {
        return getDataStreamDataTopicID(
            outputInterface.getParentProducer().getUniqueIdentifier(),
            outputInterface.getName());
    }
    
    
    public static final String getDataStreamDataTopicID(IDataStreamInfo dsInfo)
    {
        return getDataStreamDataTopicID(
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName());
    }
    
    
    static final String getCommandStreamTopicID(String procedureUID, String controlInputName)
    {
        OshAsserts.checkValidUID(procedureUID);
        Asserts.checkNotNullOrEmpty(controlInputName, "controlInputName");
        return PROCEDURE_TOPIC_PREFIX + procedureUID + PROCEDURE_CONTROL_CHANNELS + controlInputName;
    }
    
    
    public static final String getCommandStreamStatusTopicID(String procedureUID, String controlInputName)
    {
        return getCommandStreamTopicID(procedureUID, controlInputName) + STATUS_CHANNEL;
    }
    
    
    public static final String getCommandStreamStatusTopicID(IStreamingControlInterface controlInterface)
    {
        return getCommandStreamStatusTopicID(
            controlInterface.getParentProducer().getUniqueIdentifier(),
            controlInterface.getName());
    }
    
    
    public static final String getCommandStreamStatusTopicID(ICommandStreamInfo csInfo)
    {
        return getCommandStreamStatusTopicID(
            csInfo.getProcedureID().getUniqueID(),
            csInfo.getCommandName());
    }
    
    
    public static final String getCommandStreamDataTopicID(String procedureUID, String controlInputName)
    {
        return getCommandStreamTopicID(procedureUID, controlInputName) + DATA_CHANNEL;
    }
    
    
    public static final String getCommandStreamDataTopicID(IStreamingControlInterface controlInterface)
    {
        return getCommandStreamDataTopicID(
            controlInterface.getParentProducer().getUniqueIdentifier(),
            controlInterface.getName());
    }
    
    
    public static final String getCommandStreamDataTopicID(ICommandStreamInfo csInfo)
    {
        return getCommandStreamDataTopicID(
            csInfo.getProcedureID().getUniqueID(),
            csInfo.getName());
    }
    
    
    public static final String getCommandStreamAckTopicID(String procedureUID, String controlInputName)
    {
        return getCommandStreamTopicID(procedureUID, controlInputName) + ACK_CHANNEL;
    }
    
    
    public static final String getCommandStreamAckTopicID(IStreamingControlInterface controlInterface)
    {
        return getCommandStreamAckTopicID(
            controlInterface.getParentProducer().getUniqueIdentifier(),
            controlInterface.getName());
    }
    
    
    public static final String getCommandStreamAckTopicID(ICommandStreamInfo csInfo)
    {
        return getCommandStreamAckTopicID(
            csInfo.getProcedureID().getUniqueID(),
            csInfo.getName());
    }
    
}
