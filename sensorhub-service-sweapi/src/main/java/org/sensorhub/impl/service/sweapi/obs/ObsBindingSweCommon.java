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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.Collection;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsData;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.obs.ObsHandler.ObsHandlerContextData;
import org.sensorhub.impl.service.sweapi.resource.BaseResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
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
import net.opengis.swe.v20.BinaryBlock;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.TextEncoding;


public class ObsBindingSweCommon extends ResourceBinding<BigInteger, IObsData>
{       
    ObsHandlerContextData contextData;
    DataStreamParser resultReader;
    DataStreamWriter resultWriter;
    ScalarIndexer timeStampIndexer;

    
    ObsBindingSweCommon(ResourceContext ctx, IdEncoder idEncoder, boolean forReading, IObsStore obsStore) throws IOException
    {
        super(ctx, idEncoder);
        this.contextData = (ObsHandlerContextData)ctx.getData();
                
        var dsInfo = contextData.dsInfo;
        if (forReading)
        {
            var is = ctx.getRequest().getInputStream();
            resultReader = getSweCommonParser(dsInfo, is, ctx.getFormat());
            timeStampIndexer = SWEHelper.getTimeStampIndexer(contextData.dsInfo.getRecordStructure());
        }
        else
        {
            var os = ctx.getResponse().getOutputStream();
            resultWriter = getSweCommonWriter(dsInfo, os, ctx.getPropertyFilter(), ctx.getFormat());
        }
    }
    
    
    @Override
    public IObsData deserialize() throws IOException
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
        return new ObsData.Builder()
            .withDataStream(contextData.dsID)
            .withFoi(contextData.foiId)
            .withPhenomenonTime(SWEDataUtils.toInstant(time))
            .withResult(rec)
            .build();
    }


    @Override
    public void serialize(BigInteger key, IObsData obs, boolean showLinks) throws IOException
    {
        resultWriter.write(obs.getResult());
        resultWriter.flush();
    }
    
    
    protected DataStreamParser getSweCommonParser(IDataStreamInfo dsInfo, InputStream is, ResourceFormat format) throws IOException
    {
        DataStreamParser dataParser = null;
        
        // init SWE datastream writer depending on desired format and native encoding
        if (dsInfo.getRecordEncoding() instanceof TextEncoding)
        {
            if (format.equals(ResourceFormat.SWE_JSON))
            {
                dataParser = new JsonDataParserGson();
            }
            else if (format.equals(ResourceFormat.SWE_TEXT))
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
            else if (allowNonBinaryFormat(dsInfo))
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
            throw new InvalidRequestException(BaseResourceHandler.UNSUPPORTED_FORMAT_ERROR_MSG + format);
        
        dataParser.setInput(is);
        dataParser.setDataComponents(dsInfo.getRecordStructure());
        
        return dataParser;
    }
    
    
    protected DataStreamWriter getSweCommonWriter(IDataStreamInfo dsInfo, OutputStream os, PropertyFilter propFilter, ResourceFormat format) throws IOException
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
            else if (allowNonBinaryFormat(dsInfo))
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
            throw new InvalidRequestException(BaseResourceHandler.UNSUPPORTED_FORMAT_ERROR_MSG + format);
        
        dataWriter.setOutput(os);
        dataWriter.setDataComponents(dsInfo.getRecordStructure());
        
        return dataWriter;
    }
    
    
    protected boolean allowNonBinaryFormat(IDataStreamInfo dsInfo)
    {
        if (dsInfo.getRecordEncoding() instanceof BinaryEncoding)
        {
            var enc = (BinaryEncoding)dsInfo.getRecordEncoding();
            for (var member: enc.getMemberList())
            {
                if (member instanceof BinaryBlock)
                    return false;
            }
        }
        
        return true;
    }


    @Override
    public void startCollection() throws IOException
    {
        resultWriter.startStream(true);
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        resultWriter.endStream();
        resultWriter.flush();
    }
}
