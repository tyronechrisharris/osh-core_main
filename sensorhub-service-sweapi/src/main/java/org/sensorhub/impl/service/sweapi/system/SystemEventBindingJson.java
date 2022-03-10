/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.system;

import static org.sensorhub.impl.service.sweapi.event.ResourceEventsHandler.*;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import org.sensorhub.api.system.SystemAddedEvent;
import org.sensorhub.api.system.SystemChangedEvent;
import org.sensorhub.api.system.SystemDisabledEvent;
import org.sensorhub.api.system.SystemEnabledEvent;
import org.sensorhub.api.system.SystemEvent;
import org.sensorhub.api.system.SystemRemovedEvent;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * JSON serializer for system events
 * </p>
 *
 * @author Alex Robin
 * @since Feb 26, 2022
 */
public class SystemEventBindingJson extends ResourceBindingJson<Long, SystemEvent>
{
    protected JsonWriter writer;
    
    
    SystemEventBindingJson(RequestContext ctx) throws IOException
    {
        super(ctx, new IdEncoder(SystemHandler.EXTERNAL_ID_SEED));
        this.writer = getJsonWriter(ctx.getOutputStream(), ctx.getPropertyFilter());
    }


    @Override
    public SystemEvent deserialize() throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void serialize(Long key, SystemEvent res, boolean showLinks) throws IOException
    {
        var eventType =
            (res instanceof SystemAddedEvent) ? RESOURCE_ADDED_EVENT_TYPE :
            (res instanceof SystemRemovedEvent) ? RESOURCE_DELETED_EVENT_TYPE :
            (res instanceof SystemChangedEvent) ? RESOURCE_UPDATED_EVENT_TYPE :
            (res instanceof SystemEnabledEvent) ? RESOURCE_ENABLED_EVENT_TYPE :
            (res instanceof SystemDisabledEvent) ? RESOURCE_DISABLED_EVENT_TYPE :
            null;
        
        if (eventType == null)
            return;
        
        // generate public ID
        var publicSysID = idEncoder.encodeID(res.getSystemID());
        
        // write event message
        writer.beginObject();
        writer.name("time").value(Instant.ofEpochMilli(res.getTimeStamp()).toString());
        writer.name("system@id").value(Long.toString(publicSysID, 36));
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
