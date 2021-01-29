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
import java.util.ArrayList;
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.procedure.ProcedureHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


public class DataStreamBindingJson extends ResourceBindingJson<DataStreamKey, IDataStreamInfo>
{
    String rootURL;
    SWEStaxBindings sweBindings;
    JsonReader reader;
    SWEJsonStreamReader sweReader;
    JsonWriter writer;
    SWEJsonStreamWriter sweWriter;
    
    
    DataStreamBindingJson(ResourceContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder);
        
        this.rootURL = ctx.getApiRootURL();
        this.sweBindings = new SWEStaxBindings();
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getRequest().getInputStream());
            this.reader = getJsonReader(is);
            this.sweReader = new SWEJsonStreamReader(reader);
        }
        else
        {
            this.writer = getJsonWriter(ctx.getResponse().getOutputStream(), ctx.getPropertyFilter());
            this.sweWriter = new SWEJsonStreamWriter(writer);
        }
    }
    
    
    @Override
    public IDataStreamInfo deserialize() throws IOException
    {
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
                
        String name = null;
        String description = null;
        DataComponent resultStruct = null;
        DataEncoding resultEncoding = new TextEncodingImpl();
        
        try
        {
            reader.beginObject();
            while (reader.hasNext())
            {
                var prop = reader.nextName();
                
                if ("name".equals(prop))
                    name = reader.nextString();
                else if ("description".equals(prop))
                    description = reader.nextString();
                else if ("resultSchema".equals(prop))
                {
                    sweReader.nextTag();
                    resultStruct = sweBindings.readDataComponent(sweReader);
                }
                else if ("resultEncoding".equals(prop))
                {
                    sweReader.nextTag();
                    resultEncoding = sweBindings.readAbstractEncoding(sweReader);
                }
                else
                    reader.skipValue();
            }
            reader.endObject();
        }
        catch (XMLStreamException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        catch (IllegalStateException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        if (resultStruct == null)
            throw new ResourceParseException("Missing resultSchema");
        
        // set name and description inside data component
        if (name != null)
            resultStruct.setName(name);
        if (description != null)
            resultStruct.setDescription(description);
        
        var dsInfo = new DataStreamInfo.Builder()
            .withProcedure(new ProcedureId(1, "temp-uid")) // use dummy UID since it will be replaced later
            .withRecordDescription(resultStruct)
            .withRecordEncoding(resultEncoding)
            .build();
        
        return dsInfo;
    }


    @Override
    public void serialize(DataStreamKey key, IDataStreamInfo dsInfo, boolean showLinks) throws IOException
    {
        var publicDsID = encodeID(key.getInternalID());
        var publicProcID = encodeID(dsInfo.getProcedureID().getInternalID());
        
        writer.beginObject();
        
        writer.name("id").value(Long.toString(publicDsID, 36));
        writer.name("name").value(dsInfo.getName());
        writer.name("procedure").value(Long.toString(publicProcID, 36));
        
        writer.name("validTime").beginArray()
            .value(dsInfo.getValidTime().begin().toString())
            .value(dsInfo.getValidTime().end().toString())
            .endArray();

        if (dsInfo.getPhenomenonTimeRange() != null)
        {
            writer.name("phenomenonTime").beginArray()
                .value(dsInfo.getPhenomenonTimeRange().begin().toString())
                .value(dsInfo.getPhenomenonTimeRange().end().toString())
                .endArray();
        }

        if (dsInfo.getResultTimeRange() != null)
        {
            writer.name("resultTime").beginArray()
                .value(dsInfo.getResultTimeRange().begin().toString())
                .value(dsInfo.getResultTimeRange().end().toString())
                .endArray();
        }
        
        if (dsInfo.hasDiscreteResultTimes())
        {
            writer.name("runTimes").beginArray();
            for (var time: dsInfo.getDiscreteResultTimes().keySet())
                writer.value(time.toString());
            writer.endArray();
        }
        
        // result structure & encoding
        try
        {
            writer.name("resultSchema");
            sweWriter.resetContext();
            sweBindings.writeDataComponent(sweWriter, dsInfo.getRecordStructure(), false);
            
            writer.name("resultEncoding");
            sweWriter.resetContext();
            sweBindings.writeAbstractEncoding(sweWriter, dsInfo.getRecordEncoding());
        }
        catch (Exception e)
        {
            throw new IOException("Error writing SWE Common result structure", e);
        }
        
        if (showLinks)
        {
            var links = new ArrayList<ResourceLink>();
                        
            links.add(new ResourceLink.Builder()
                .rel("procedure")
                .title("Parent procedure")
                .href(rootURL +
                      "/" + ProcedureHandler.NAMES[0] +
                      "/" + Long.toString(publicProcID, 36))
                .build());
            
            links.add(new ResourceLink.Builder()
                .rel("observations")
                .title("Collection of observations")
                .href(rootURL +
                      "/" + DataStreamHandler.NAMES[0] +
                      "/" + Long.toString(publicDsID, 36) +
                      "/" + ObsHandler.NAMES[0])
                .build());
            
            writeLinksAsJson(writer, links);
        }
        
        writer.endObject();
        writer.flush();
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
