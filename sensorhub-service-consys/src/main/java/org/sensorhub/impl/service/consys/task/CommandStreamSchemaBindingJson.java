/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.task;

import java.io.IOException;
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.system.SystemId;
import org.sensorhub.impl.service.consys.ResourceParseException;
import org.sensorhub.impl.service.consys.SWECommonUtils;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceBindingJson;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.resource.ResourceLink;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


public class CommandStreamSchemaBindingJson extends ResourceBindingJson<CommandStreamKey, ICommandStreamInfo>
{
    String rootURL;
    SWEStaxBindings sweBindings;
    SWEJsonStreamReader sweReader;
    SWEJsonStreamWriter sweWriter;
    
    
    CommandStreamSchemaBindingJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
        init(ctx, forReading);
    }
    
    
    CommandStreamSchemaBindingJson(RequestContext ctx, IdEncoders idEncoders, JsonReader reader) throws IOException
    {
        super(ctx, idEncoders, reader);
        init(ctx, true);
    }
    
    
    void init(RequestContext ctx, boolean forReading)
    {
        this.rootURL = ctx.getApiRootURL();
        this.sweBindings = new SWEStaxBindings();
        
        if (forReading)
            this.sweReader = new SWEJsonStreamReader(reader);
        else
            this.sweWriter = new SWEJsonStreamWriter(writer);
    }
    
    
    @Override
    public ICommandStreamInfo deserialize(JsonReader reader) throws IOException
    {
        DataComponent commandStruct = null;
        DataComponent resultStruct = null;
        DataEncoding commandEncoding = new TextEncodingImpl();
        DataEncoding resultEncoding = new TextEncodingImpl();
        
        try
        {
            // read BEGIN_OBJECT only if not already read by caller
            // this happens when reading embedded schema and auto-detecting command format
            if (reader.peek() == JsonToken.BEGIN_OBJECT)
                reader.beginObject();
            
            while (reader.hasNext())
            {
                var prop = reader.nextName();
                
                if ("commandSchema".equals(prop))
                {
                    sweReader.nextTag();
                    commandStruct = sweBindings.readDataComponent(sweReader);
                    commandStruct.setName(SWECommonUtils.NO_NAME);
                }
                else if ("commandEncoding".equals(prop))
                {
                    sweReader.nextTag();
                    commandEncoding = sweBindings.readAbstractEncoding(sweReader);
                }
                else if ("resultSchema".equals(prop))
                {
                    sweReader.nextTag();
                    resultStruct = sweBindings.readDataComponent(sweReader);
                    resultStruct.setName(SWECommonUtils.NO_NAME);
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
        catch (XMLStreamException | IllegalStateException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        return new CommandStreamInfo.Builder()
            .withName(SWECommonUtils.NO_NAME) // name will be set later
            .withSystem(SystemId.NO_SYSTEM_ID) // System ID will be set later
            .withRecordDescription(commandStruct)
            .withRecordEncoding(commandEncoding)
            .withResultDescription(resultStruct)
            .withResultEncoding(resultEncoding)
            .build();
    }


    @Override
    public void serialize(CommandStreamKey key, ICommandStreamInfo dsInfo, boolean showLinks, JsonWriter writer) throws IOException
    {
        var dsId = idEncoders.getCommandStreamIdEncoder().encodeID(key.getInternalID());
        
        writer.beginObject();
        writer.name("control@id").value(dsId);
        writer.name("commandFormat").value(ResourceFormat.JSON.toString());
        
        // param structure & encoding
        try
        {
            writer.name("paramsSchema");
            sweWriter.resetContext();
            sweBindings.writeDataComponent(sweWriter, dsInfo.getRecordStructure(), false);
        }
        catch (Exception e)
        {
            throw new IOException("Error writing command structure", e);
        }
        
        // result structure & encoding
        if (dsInfo.hasInlineResult())
        {
            try
            {
                writer.name("resultSchema");
                sweWriter.resetContext();
                sweBindings.writeDataComponent(sweWriter, dsInfo.getResultStructure(), false);
            }
            catch (Exception e)
            {
                throw new IOException("Error writing command structure", e);
            }
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
