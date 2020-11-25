/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.io.IOException;
import javax.servlet.AsyncContext;
import org.sensorhub.impl.service.swe.RecordTemplate;
import org.vast.ows.sos.GetResultRequest;
import org.vast.ows.sos.SOSException;
import net.opengis.swe.v20.BinaryEncoding;


/**
 * <p>
 * Result serializer implementation that automatically selects format
 * based on native encoding and structure.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2020
 */
public class ResultSerializerAuto extends AbstractResultSerializerSwe
{
    AbstractResultSerializerSwe serializer;
    
    
    @Override
    public void init(SOSServlet servlet, AsyncContext asyncCtx, GetResultRequest req, RecordTemplate resultTemplate) throws SOSException, IOException
    {
        // select serializer depending on encoding type
        if (resultTemplate.getDataEncoding() instanceof BinaryEncoding)
            serializer = new ResultSerializerBinary();
        else
            serializer = new ResultSerializerText();
        
        serializer.init(servlet, asyncCtx, req, resultTemplate);
    }


    @Override
    protected void beforeRecords() throws IOException
    {
        serializer.beforeRecords();
    }


    @Override
    protected void afterRecords() throws IOException
    {
        serializer.afterRecords();
    }

}
