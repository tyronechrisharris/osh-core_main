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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import org.sensorhub.api.command.CommandData;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.task.CommandHandler.CommandHandlerContextData;
import org.sensorhub.utils.SWEDataUtils;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.vast.cdm.common.DataStreamParser;
import org.vast.cdm.common.DataStreamWriter;
import org.vast.data.TextEncodingImpl;
import org.vast.data.XMLEncodingImpl;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.swe.XmlDataParser;
import org.vast.swe.AsciiDataParser;
import org.vast.swe.fast.JsonDataParserGson;
import org.vast.swe.fast.JsonDataWriter;
import org.vast.swe.fast.JsonDataWriterGson;
import org.vast.swe.fast.TextDataWriter;
import org.vast.swe.fast.XmlDataWriter;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.TextEncoding;


public class CommandBindingSweCommon extends ResourceBinding<BigId, ICommandData>
{       
    CommandHandlerContextData contextData;
    DataStreamParser resultReader;
    DataStreamWriter paramsWriter;
    ScalarIndexer timeStampIndexer;
    String userID;

    
    CommandBindingSweCommon(RequestContext ctx, IdEncoders idEncoders, boolean forReading, ICommandStore obsStore) throws IOException
    {
        super(ctx, idEncoders);
        this.contextData = (CommandHandlerContextData)ctx.getData();
        
        var dsInfo = contextData.dsInfo;
        if (forReading)
        {
            var is = ctx.getInputStream();
            resultReader = getSweCommonParser(dsInfo, is, ctx.getFormat());
            timeStampIndexer = SWEHelper.getTimeStampIndexer(contextData.dsInfo.getRecordStructure());
            
            var user = ctx.getSecurityHandler().getCurrentUser();
            this.userID = user != null ? user.getId() : "api";
        }
        else
        {
            var os = ctx.getOutputStream();
            paramsWriter = getSweCommonWriter(dsInfo, os, ctx.getPropertyFilter(), ctx.getFormat());
            
            // if request is coming from a browser, use well-known mime type
            // so browser can display the response
            if (ctx.isBrowserHtmlRequest())
            {
                if (ctx.getFormat().equals(ResourceFormat.SWE_TEXT))
                    ctx.setResponseContentType(ResourceFormat.TEXT_PLAIN.getMimeType());
                else if (ctx.getFormat().equals(ResourceFormat.SWE_XML))
                    ctx.setResponseContentType(ResourceFormat.APPLI_XML.getMimeType());
                else
                    ctx.setResponseContentType(ctx.getFormat().getMimeType());
            }
            else
                ctx.setResponseContentType(ctx.getFormat().getMimeType());
        }
    }
    
    
    @Override
    public ICommandData deserialize() throws IOException
    {
        var rec = resultReader.parseNextBlock();
        if (rec == null)
            return null;
        
        // get time stamp
        double time;
        if (timeStampIndexer != null)
            time = timeStampIndexer.getDoubleValue(rec);
        else
            time = System.currentTimeMillis() / 1000.;
        
        // create obs object
        return new CommandData.Builder()
            .withCommandStream(contextData.streamID)
            .withSender(userID)
            //.withFoi(contextData.foiId)
            .withIssueTime(SWEDataUtils.toInstant(time))
            .withParams(rec)
            .build();
    }


    @Override
    public void serialize(BigId key, ICommandData cmd, boolean showLinks) throws IOException
    {
        paramsWriter.write(cmd.getParams());
        paramsWriter.flush();
    }
    
    
    protected DataStreamParser getSweCommonParser(ICommandStreamInfo dsInfo, InputStream is, ResourceFormat format) throws IOException
    {
        DataStreamParser dataParser = null;
        
        // init SWE parser depending on desired format and native encoding
        if (dsInfo.getRecordEncoding() instanceof TextEncoding)
        {
            if (format.equals(ResourceFormat.SWE_JSON))
            {
                dataParser = new JsonDataParserGson();
            }
            else if (format.isOneOf(ResourceFormat.SWE_TEXT))
            {
                dataParser = new AsciiDataParser();
                dataParser.setDataEncoding(dsInfo.getRecordEncoding());
            } 
            else if (format.equals(ResourceFormat.SWE_XML))
            {
                dataParser = new XmlDataParser();
                dataParser.setDataEncoding(new XMLEncodingImpl());
            }
            else if (format.equals(ResourceFormat.SWE_BINARY))
            {
                var defaultBinaryEncoding = SWEHelper.getDefaultBinaryEncoding(dsInfo.getRecordStructure());
                dataParser = SWEHelper.createDataParser(defaultBinaryEncoding);
            }
        }
        else if (dsInfo.getRecordEncoding() instanceof BinaryEncoding)
        {
            if (format.isOneOf(ResourceFormat.SWE_BINARY))
            {
                dataParser = SWEHelper.createDataParser(dsInfo.getRecordEncoding());
            }
            else if (ResourceFormat.allowNonBinaryFormat(dsInfo.getRecordEncoding()))
            {
                if (format.isOneOf(ResourceFormat.SWE_TEXT))
                {
                    dataParser = new AsciiDataParser();
                    dataParser.setDataEncoding(new TextEncodingImpl());
                }
                else if (format.isOneOf(ResourceFormat.SWE_JSON))
                {
                    dataParser = new JsonDataParserGson();
                }
                else if (format.isOneOf(ResourceFormat.SWE_XML))
                {
                    dataParser = new XmlDataParser();
                }
            }
        }
        
        if (dataParser == null)
            throw ServiceErrors.unsupportedFormat(format);
        
        dataParser.setInput(is);
        dataParser.setDataComponents(dsInfo.getRecordStructure());
        
        return dataParser;
    }
    
    
    protected DataStreamWriter getSweCommonWriter(ICommandStreamInfo dsInfo, OutputStream os, PropertyFilter propFilter, ResourceFormat format) throws IOException
    {
        DataStreamWriter dataWriter = null;
        
        // init SWE datastream writer depending on desired format and native encoding 
        if (dsInfo.getRecordEncoding() instanceof TextEncoding)
        {
            if (format.equals(ResourceFormat.SWE_JSON))
            {
                dataWriter = new JsonDataWriterGson();
            }
            else if (format.isOneOf(ResourceFormat.SWE_TEXT, ResourceFormat.TEXT_PLAIN, ResourceFormat.TEXT_CSV))
            {
                //dataWriter = SWEHelper.createDataWriter(dsInfo.getRecordEncoding());
                dataWriter = new TextDataWriter();
                dataWriter.setDataEncoding(dsInfo.getRecordEncoding());
            } 
            else if (format.isOneOf(ResourceFormat.SWE_XML, ResourceFormat.TEXT_XML))
            {
                dataWriter = new XmlDataWriter();
                dataWriter.setDataEncoding(new XMLEncodingImpl());
            }
            else if (format.equals(ResourceFormat.SWE_BINARY))
            {
                var defaultBinaryEncoding = SWEHelper.getDefaultBinaryEncoding(dsInfo.getRecordStructure());
                dataWriter = SWEHelper.createDataWriter(defaultBinaryEncoding);
            }
        }
        else if (dsInfo.getRecordEncoding() instanceof BinaryEncoding)
        {
            if (format.equals(ResourceFormat.SWE_BINARY))
            {
                dataWriter = SWEHelper.createDataWriter(dsInfo.getRecordEncoding());
            }
            else if (ResourceFormat.allowNonBinaryFormat(dsInfo.getRecordEncoding()))
            {
                if (format.equals(ResourceFormat.SWE_JSON))
                {
                    dataWriter = new JsonDataWriter();
                }
                else if (format.isOneOf(ResourceFormat.SWE_TEXT, ResourceFormat.TEXT_PLAIN, ResourceFormat.TEXT_CSV))
                {
                    dataWriter = new TextDataWriter();
                    dataWriter.setDataEncoding(new TextEncodingImpl());
                }
                else if (format.isOneOf(ResourceFormat.SWE_XML, ResourceFormat.TEXT_XML))
                {
                    dataWriter = new XmlDataWriter();
                }
            }
        }
        
        if (dataWriter == null)
            throw ServiceErrors.unsupportedFormat(format);
        
        dataWriter.setOutput(os);
        dataWriter.setDataComponents(dsInfo.getRecordStructure());
        
        return dataWriter;
    }


    @Override
    public void startCollection() throws IOException
    {
        paramsWriter.startStream(true);
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        paramsWriter.endStream();
        paramsWriter.flush();
    }
}
