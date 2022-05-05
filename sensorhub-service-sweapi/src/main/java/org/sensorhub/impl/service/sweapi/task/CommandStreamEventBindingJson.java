/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.task;

import static org.sensorhub.impl.service.sweapi.event.ResourceEventsHandler.*;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import org.sensorhub.api.command.CommandStreamAddedEvent;
import org.sensorhub.api.command.CommandStreamChangedEvent;
import org.sensorhub.api.command.CommandStreamDisabledEvent;
import org.sensorhub.api.command.CommandStreamEnabledEvent;
import org.sensorhub.api.command.CommandStreamEvent;
import org.sensorhub.api.command.CommandStreamRemovedEvent;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * JSON serializer for command stream events
 * </p>
 *
 * @author Alex Robin
 * @since Feb 26, 2022
 */
public class CommandStreamEventBindingJson extends ResourceBindingJson<Long, CommandStreamEvent>
{
    
    CommandStreamEventBindingJson(RequestContext ctx, IdEncoder idEncoder) throws IOException
    {
        super(ctx, idEncoder, false);
    }


    @Override
    public CommandStreamEvent deserialize(JsonReader reader) throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void serialize(Long key, CommandStreamEvent res, boolean showLinks, JsonWriter writer) throws IOException
    {
        var eventType =
            (res instanceof CommandStreamAddedEvent) ? RESOURCE_ADDED_EVENT_TYPE :
            (res instanceof CommandStreamRemovedEvent) ? RESOURCE_DELETED_EVENT_TYPE :
            (res instanceof CommandStreamChangedEvent) ? RESOURCE_UPDATED_EVENT_TYPE :
            (res instanceof CommandStreamEnabledEvent) ? RESOURCE_ENABLED_EVENT_TYPE :
            (res instanceof CommandStreamDisabledEvent) ? RESOURCE_DISABLED_EVENT_TYPE :
            null;
        
        if (eventType == null)
            return;
        
        // write event message
        writer.beginObject();
        writer.name("time").value(Instant.ofEpochMilli(res.getTimeStamp()).toString());
        writer.name("control@id").value(encodeID(res.getCommandStreamID()));
        writer.name("system@id").value(encodeID(res.getSystemID()));
        writer.name("eventType").value(eventType);
        writer.endObject();
        writer.flush();
    }


    @Override
    public void startCollection() throws IOException
    {
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
    }
}
