/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import static org.sensorhub.impl.service.sweapi.event.ResourceEventsHandler.*;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import org.sensorhub.api.data.DataStreamAddedEvent;
import org.sensorhub.api.data.DataStreamChangedEvent;
import org.sensorhub.api.data.DataStreamDisabledEvent;
import org.sensorhub.api.data.DataStreamEnabledEvent;
import org.sensorhub.api.data.DataStreamEvent;
import org.sensorhub.api.data.DataStreamRemovedEvent;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * JSON serializer for datastream events
 * </p>
 *
 * @author Alex Robin
 * @since Feb 26, 2022
 */
public class DataStreamEventBindingJson extends ResourceBindingJson<Long, DataStreamEvent>
{
    protected JsonWriter writer;
    IdEncoder sysIdEncoder;
    
    
    DataStreamEventBindingJson(RequestContext ctx) throws IOException
    {
        super(ctx, new IdEncoder(DataStreamHandler.EXTERNAL_ID_SEED));
        this.writer = getJsonWriter(ctx.getOutputStream(), ctx.getPropertyFilter());
        this.sysIdEncoder = new IdEncoder(SystemHandler.EXTERNAL_ID_SEED);
    }


    @Override
    public DataStreamEvent deserialize() throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void serialize(Long key, DataStreamEvent res, boolean showLinks) throws IOException
    {
        var eventType =
            (res instanceof DataStreamAddedEvent) ? RESOURCE_ADDED_EVENT_TYPE :
            (res instanceof DataStreamRemovedEvent) ? RESOURCE_DELETED_EVENT_TYPE :
            (res instanceof DataStreamChangedEvent) ? RESOURCE_UPDATED_EVENT_TYPE :
            (res instanceof DataStreamEnabledEvent) ? RESOURCE_ENABLED_EVENT_TYPE :
            (res instanceof DataStreamDisabledEvent) ? RESOURCE_DISABLED_EVENT_TYPE :
            null;
        
        if (eventType == null)
            return;
        
        writer.beginObject();
        writer.name("time").value(Instant.ofEpochMilli(res.getTimeStamp()).toString());
        writer.name("datastream@id").value(res.getOutputName());
        writer.name("system@id").value(res.getSystemUID());
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
