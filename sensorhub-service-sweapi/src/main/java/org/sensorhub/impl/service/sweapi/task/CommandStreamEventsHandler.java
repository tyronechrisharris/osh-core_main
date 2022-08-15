/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.task;

import java.io.IOException;
import org.sensorhub.api.command.CommandStreamEvent;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ObsSystemDbWrapper;
import org.sensorhub.impl.service.sweapi.RestApiServlet.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.event.ResourceEventsHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;


public class CommandStreamEventsHandler extends ResourceEventsHandler<CommandStreamEvent>
{
    final ISystemDescStore sysStore;
    
    
    protected CommandStreamEventsHandler(IEventBus eventBus, ObsSystemDbWrapper db, ResourcePermissions permissions)
    {
        super("control channel", eventBus, db.getIdEncoders(), permissions);
        this.sysStore = db.getReadDb().getSystemDescStore();
    }
    

    @Override
    public void subscribe(RequestContext ctx) throws InvalidRequestException, IOException
    {
        var queryParams = ctx.getParameterMap();
        //var filter = getFilter(ctx.getParentRef(), queryParams, 0, Long.MAX_VALUE);
        var responseFormat = parseFormat(queryParams);
        ctx.setFormatOptions(responseFormat, parseSelectArg(queryParams));
        var serializer = new CommandStreamEventBindingJson(ctx, idEncoders);
        
        // use registry topic if all data stream events are requested
        // otherwise use specific system topic
        String topic = null;
        if (ctx.getParentID() != null)
        {
            var sysId = ctx.getParentID();
            var sys = sysStore.getCurrentVersion(sysId);
            topic = EventUtils.getSystemStatusTopicID(sys.getUniqueIdentifier());
        }
        else
            topic = EventUtils.getSystemRegistryTopicID();
        
        var subscriptionBuilder = eventBus.newSubscription(CommandStreamEvent.class)
            .withTopicID(topic)
            .withEventType(CommandStreamEvent.class);
        
        subscribe(ctx, subscriptionBuilder, serializer);
    }

}
