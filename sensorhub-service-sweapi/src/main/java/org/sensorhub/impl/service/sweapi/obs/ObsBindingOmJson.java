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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsData;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.obs.ObsHandler.ObsHandlerContextData;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.cdm.common.DataStreamWriter;
import org.vast.swe.BinaryDataWriter;
import org.vast.swe.IComponentFilter;
import org.vast.swe.SWEConstants;
import org.vast.swe.fast.JsonDataParserGson;
import org.vast.swe.fast.JsonDataWriterGson;
import org.vast.util.ReaderException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.BinaryBlock;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.DataComponent;


public class ObsBindingOmJson extends ResourceBindingJson<BigInteger, IObsData>
{
    ObsHandlerContextData contextData;
    IObsStore obsStore;
    JsonReader reader;
    JsonWriter writer;
    JsonDataParserGson resultReader;
    Map<Long, DataStreamWriter> resultWriters;
    
    
    ObsBindingOmJson(ResourceContext ctx, IdEncoder idEncoder, boolean forReading, IObsStore obsStore) throws IOException
    {
        super(ctx, idEncoder);
        this.contextData = (ObsHandlerContextData)ctx.getData();
        this.obsStore = obsStore;
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getRequest().getInputStream());
            this.reader = getJsonReader(is);
            resultReader = getSweCommonParser(contextData.dsInfo, reader);
        }
        else
        {
            var os = ctx.isWebSocket() ? ctx.getWebsocketOutputStream() : ctx.getResponse().getOutputStream();
            this.writer = getJsonWriter(os, ctx.getPropertyFilter());
            this.resultWriters = new HashMap<>();
            
            // init result writer only in case of single datastream
            // otherwise we'll do it later
            if (contextData.dsInfo != null)
            {
                var resultWriter = getSweCommonWriter(contextData.dsInfo, writer, ctx.getPropertyFilter());
                resultWriters.put(ctx.getParentID(), resultWriter);
            }
        }
    }
    
    
    @Override
    public IObsData deserialize() throws IOException
    {
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
        
        var obs = new ObsData.Builder()
            .withDataStream(contextData.dsID);
        
        try
        {
            reader.beginObject();
            
            while (reader.hasNext())
            {
                var propName = reader.nextName();
                
                if ("phenomenonTime".equals(propName))
                    obs.withPhenomenonTime(Instant.parse(reader.nextString()));
                else if ("resultTime".equals(propName))
                    obs.withResultTime(Instant.parse(reader.nextString()));
                //else if ("foi".equals(propName))
                //    obs.withFoi(id)
                else if ("result".equals(propName))
                {
                    var result = resultReader.parseNextBlock();
                    obs.withResult(result);
                }
                else
                    reader.skipValue();
            }
            
            reader.endObject();
        }
        catch (IllegalStateException | ReaderException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        if (contextData.foiId != null)
            obs.withFoi(contextData.foiId);
        
        return obs.build();
    }


    @Override
    public void serialize(BigInteger key, IObsData obs, boolean showLinks) throws IOException
    {
        writer.beginObject();
        
        if (key != null)
            writer.name("id").value(key.toString(ResourceBinding.ID_RADIX));
        
        var dsID = obs.getDataStreamID();
        var externalDsId = encodeID(dsID);
        writer.name("datastream").value(Long.toString(externalDsId, ResourceBinding.ID_RADIX));
        
        if (obs.getFoiID() != null && obs.getFoiID() != FeatureId.NULL_FEATURE)
        {
            var externalfoiId = encodeID(obs.getFoiID().getInternalID());
            writer.name("foi").value(Long.toString(externalfoiId, ResourceBinding.ID_RADIX));
        }
        
        writer.name("phenomenonTime").value(obs.getPhenomenonTime().toString());
        writer.name("resultTime").value(obs.getResultTime().toString());
        
        // create or reuse existing result writer and write result data
        writer.name("result");
        var resultWriter = resultWriters.computeIfAbsent(dsID,
            k -> getSweCommonWriter(k, writer, ctx.getPropertyFilter()) );
        
        // write if JSON is supported, otherwise print warning message
        if (resultWriter instanceof JsonDataWriterGson)
            resultWriter.write(obs.getResult());
        else
            writer.value("Compressed binary result not shown in JSON");
        
        writer.endObject();
        writer.flush();
    }
    
    
    protected DataStreamWriter getSweCommonWriter(long dsID, JsonWriter writer, PropertyFilter propFilter)
    {
        var dsInfo = obsStore.getDataStreams().get(new DataStreamKey(dsID));
        return getSweCommonWriter(dsInfo, writer, propFilter);
    }
    
    
    protected DataStreamWriter getSweCommonWriter(IDataStreamInfo dsInfo, JsonWriter writer, PropertyFilter propFilter)
    {        
        if (!allowNonBinaryFormat(dsInfo))
            return new BinaryDataWriter();
        
        // create JSON SWE writer
        var sweWriter = new JsonDataWriterGson(writer);
        sweWriter.setDataComponents(dsInfo.getRecordStructure());
        
        // filter out time component since it's already included in O&M
        sweWriter.setDataComponentFilter(new IComponentFilter() {
            @Override
            public boolean accept(DataComponent comp)
            {
                if (comp.getParent() == null ||
                    SWEConstants.DEF_PHENOMENON_TIME.equals(comp.getDefinition()) ||
                    SWEConstants.DEF_SAMPLING_TIME.equals(comp.getDefinition()))
                    return false;
                else
                    return true;
            }            
        });
        
        return sweWriter;
    }
    
    
    protected JsonDataParserGson getSweCommonParser(IDataStreamInfo dsInfo, JsonReader reader)
    {
        var sweParser = new JsonDataParserGson(reader);
        sweParser.setDataComponents(dsInfo.getRecordStructure());
        return sweParser;
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
        startJsonCollection(writer);        
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        endJsonCollection(writer, links);        
    }
}
