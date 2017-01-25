/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2016-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import java.io.ByteArrayInputStream;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.sensorhub.impl.service.swe.WebSocketUtils;
import org.slf4j.Logger;
import org.vast.cdm.common.DataStreamParser;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Input only websocket for receiving live command streams (for direct tasking)
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Jan 24, 2017
 */
public class SPSWebSocketIn implements WebSocketListener
{
    final static String TEXT_NOT_SUPPORTED = "Incoming text data is not supported";
    final static String WS_ERROR = "Error while processing incoming websocket stream";
    
    SPSServlet service;
    Logger log;
    Session session;
    DataStreamParser parser;
    ISPSConnector connector;
    ITask task;
    
    
    public SPSWebSocketIn(SPSServlet service, ITask task, DataStreamParser parser, ISPSConnector connector)
    {
        this.service = service;
        this.log = service.log;
        this.task = task;
        this.parser = parser;
        this.connector = connector;
    }
    
    
    @Override
    public void onWebSocketConnect(Session session)
    {
        this.session = session;
    }
    
    
    @Override
    public void onWebSocketBinary(byte payload[], int offset, int len)
    {
        try
        {
            // skip if no payload
            if (payload == null || payload.length == 0)
                return;
            
            ByteArrayInputStream is = new ByteArrayInputStream(payload, offset, len);
            parser.setInput(is);
            DataBlock data = parser.parseNextBlock();
            connector.sendSubmitData(task, data);
        }
        catch (Exception e)
        {
            log.error("Error while parsing websocket packet", e);
            if (session != null)
                session.close(StatusCode.BAD_DATA, e.getMessage());
        }
    }


    @Override
    public void onWebSocketClose(int statusCode, String reason)
    {
        WebSocketUtils.logClose(statusCode, reason, log);
        service.cleanupSession(task.getID());        
        session = null;
    }
    
    
    @Override
    public void onWebSocketError(Throwable e)
    {
        log.error(WS_ERROR, e);
    }


    @Override
    public void onWebSocketText(String msg)
    {
        session.close(StatusCode.BAD_DATA, TEXT_NOT_SUPPORTED);
    }
    
    
    public void close()
    {
        session.close(StatusCode.NORMAL, "End of tasking session");
        service.cleanupSession(task.getID());
    }
}
