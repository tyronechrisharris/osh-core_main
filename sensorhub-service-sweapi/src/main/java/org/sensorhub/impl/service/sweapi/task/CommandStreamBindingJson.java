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
import java.util.ArrayList;
import java.util.Collection;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.SWECommonUtils;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


public class CommandStreamBindingJson extends ResourceBindingJson<CommandStreamKey, ICommandStreamInfo>
{
    String rootURL;
    SWEStaxBindings sweBindings;
    SWEJsonStreamReader sweReader;
    SWEJsonStreamWriter sweWriter;
    
    
    CommandStreamBindingJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
        
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
        // if array, prepare to parse first element
        if (reader.peek() == JsonToken.BEGIN_ARRAY)
            reader.beginArray();
        
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
                
        String name = null;
        String description = null;
        String inputName = null;
        ICommandStreamInfo csInfo = null;
        
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
                else if ("inputName".equals(prop))
                    inputName = reader.nextString();
                else if ("schema".equals(prop))
                {
                    reader.beginObject();
                    
                    /*// obsFormat must come first!
                    if (!reader.nextName().equals("commandFormat"))
                        throw new ResourceParseException(MISSING_PROP_ERROR_MSG + "schema/obsFormat");
                    var obsFormat = reader.nextString();
                    
                    ResourceBindingJson<DataStreamKey, IDataStreamInfo> schemaBinding = null;
                    if (ResourceFormat.JSON.getMimeType().equals(obsFormat))
                        schemaBinding = new CommandStreamSchemaBindingJson(ctx, idEncoders, reader);
                    
                    if (schemaBinding == null)
                        throw ServiceErrors.unsupportedFormat(obsFormat);*/
                    var schemaBinding = new CommandStreamSchemaBindingJson(ctx, idEncoders, reader);
                    csInfo = schemaBinding.deserialize(reader);
                }
                else
                    reader.skipValue();
            }
            reader.endObject();
        }
        catch (InvalidRequestException | ResourceParseException e)
        {
            throw e;
        }
        catch (IOException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        // check that mandatory properties have been parsed
        if (inputName == null)
            throw new ResourceParseException(MISSING_PROP_ERROR_MSG + "inputName");
        if (csInfo == null)
            throw new ResourceParseException(MISSING_PROP_ERROR_MSG + "schema");
        
        // assign outputName to data component
        csInfo.getRecordStructure().setName(inputName);
        
        // create datastream info object
        return CommandStreamInfo.Builder.from(csInfo)
            .withName(name)
            .withDescription(description)
            .build();
    }


    @Override
    public void serialize(CommandStreamKey key, ICommandStreamInfo csInfo, boolean showLinks, JsonWriter writer) throws IOException
    {
        var dsId = idEncoders.getCommandStreamIdEncoder().encodeID(key.getInternalID());
        var sysId = idEncoders.getSystemIdEncoder().encodeID(csInfo.getSystemID().getInternalID());
        
        writer.beginObject();
        
        writer.name("id").value(dsId);
        writer.name("name").value(csInfo.getName());
        
        if (csInfo.getDescription() != null)
            writer.name("description").value(csInfo.getDescription());
        
        writer.name("system@id").value(sysId);
        writer.name("inputName").value(csInfo.getControlInputName());
        
        writer.name("validTime").beginArray()
            .value(csInfo.getValidTime().begin().toString())
            .value(csInfo.getValidTime().end().toString())
            .endArray();

        if (csInfo.getExecutionTimeRange() != null)
        {
            writer.name("actuationTime").beginArray()
                .value(csInfo.getExecutionTimeRange().begin().toString())
                .value(csInfo.getExecutionTimeRange().end().toString())
                .endArray();
        }

        if (csInfo.getIssueTimeRange() != null)
        {
            writer.name("issueTime").beginArray()
                .value(csInfo.getIssueTimeRange().begin().toString())
                .value(csInfo.getIssueTimeRange().end().toString())
                .endArray();
        }
        
        // observed properties
        writer.name("actuableProperties").beginArray();
        for (var prop: SWECommonUtils.getProperties(csInfo.getRecordStructure()))
        {
            writer.beginObject();
            if (prop.getDefinition() != null)
                writer.name("definition").value(prop.getDefinition());
            if (prop.getLabel() != null)
                writer.name("label").value(prop.getLabel());
            if (prop.getDescription() != null)
                writer.name("description").value(prop.getDescription());
            writer.endObject();
        }
        writer.endArray();
        
        // available formats
        writer.name("formats").beginArray();
        writer.value(ResourceFormat.JSON.getMimeType());
        if (ResourceFormat.allowNonBinaryFormat(csInfo.getRecordEncoding()))
        {
            writer.value(ResourceFormat.SWE_JSON.getMimeType());
            writer.value(ResourceFormat.SWE_TEXT.getMimeType());
            writer.value(ResourceFormat.SWE_XML.getMimeType());
        }
        writer.value(ResourceFormat.SWE_BINARY.getMimeType());
        writer.endArray();
        
        if (showLinks)
        {
            var links = new ArrayList<ResourceLink>();
                        
            links.add(new ResourceLink.Builder()
                .rel("system")
                .title("Parent system")
                .href(rootURL +
                      "/" + SystemHandler.NAMES[0] +
                      "/" + sysId)
                .build());
            
            links.add(new ResourceLink.Builder()
                .rel("commands")
                .title("Collection of commands")
                .href(rootURL +
                      "/" + CommandStreamHandler.NAMES[0] +
                      "/" + dsId +
                      "/" + CommandHandler.NAMES[0])
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
