/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import java.io.IOException;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.sensorhub.impl.sensor.swe.ITaskingCallback;
import org.sensorhub.impl.service.swe.WebSocketOutputStream;
import org.sensorhub.impl.service.swe.WebSocketUtils;
import org.slf4j.Logger;
import org.vast.cdm.common.DataStreamWriter;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Output only websocket for sending live SPS commands to registered sensor
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Dec 20, 2016
 */
public class SPSWebSocketOut extends WebSocketAdapter implements ITaskingCallback 
{
    final static String WS_WRITE_ERROR = "Error while writing SWE command message";
    
    Logger log;
    DataStreamWriter writer;
    Session session;
    
    
    public SPSWebSocketOut(DataStreamWriter writer, Logger log)
    {
        this.writer = writer;
        this.log = log;
    }
    
    
    @Override
    public void onWebSocketConnect(Session session)
    {
        this.session = session;
        WebSocketUtils.logOpen(session, log);
        
        try
        {
            writer.setOutput(new WebSocketOutputStream(session, 1024));
            super.onWebSocketConnect(session);
        }
        catch (Exception e)
        {
            WebSocketUtils.closeSession(session, StatusCode.SERVER_ERROR, WebSocketUtils.INIT_ERROR, log);
        }
    }


    @Override
    public void onWebSocketError(Throwable e)
    {
        log.error(WebSocketUtils.SEND_ERROR_MSG, e);
    }
    
    
    @Override
    public void onWebSocketClose(int statusCode, String reason)
    {
        WebSocketUtils.logClose(session, statusCode, reason, log);
        super.onWebSocketClose(statusCode, reason);
    }
    
    
    @Override
    public void onWebSocketBinary(byte payload[], int offset, int len)
    {
        WebSocketUtils.closeSession(session, StatusCode.BAD_DATA, WebSocketUtils.INPUT_NOT_SUPPORTED, log);
    }


    @Override
    public void onWebSocketText(String msg)
    {
        WebSocketUtils.closeSession(session, StatusCode.BAD_DATA, WebSocketUtils.INPUT_NOT_SUPPORTED, log);
    }


    @Override
    public void onCommandReceived(DataBlock cmdData)
    {
        try
        {
            if (isConnected())
            {
                writer.write(cmdData);
                writer.flush();
            }
        }
        catch (IOException e)
        {
            log.error(WS_WRITE_ERROR, e);
            if (isConnected())
                getSession().close(StatusCode.SERVER_ERROR, WS_WRITE_ERROR);
        }
    }


    @Override
    public void onClose()
    {
        if (isConnected())
            getSession().close();
    }
}
