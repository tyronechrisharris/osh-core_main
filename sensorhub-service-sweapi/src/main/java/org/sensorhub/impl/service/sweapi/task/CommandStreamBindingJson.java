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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.system.SystemId;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.data.DataIterator;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.DataChoice;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import net.opengis.swe.v20.DataRecord;
import net.opengis.swe.v20.Vector;


public class CommandStreamBindingJson extends ResourceBindingJson<CommandStreamKey, ICommandStreamInfo>
{
    String rootURL;
    SWEStaxBindings sweBindings;
    JsonReader reader;
    SWEJsonStreamReader sweReader;
    JsonWriter writer;
    SWEJsonStreamWriter sweWriter;
    IdEncoder sysIdEncoder = new IdEncoder(SystemHandler.EXTERNAL_ID_SEED);
    
    
    CommandStreamBindingJson(RequestContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder);
        
        this.rootURL = ctx.getApiRootURL();
        this.sweBindings = new SWEStaxBindings();
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getInputStream());
            this.reader = getJsonReader(is);
            this.sweReader = new SWEJsonStreamReader(reader);
        }
        else
        {
            this.writer = getJsonWriter(ctx.getOutputStream(), ctx.getPropertyFilter());
            this.sweWriter = new SWEJsonStreamWriter(writer);
        }
    }
    
    
    @Override
    public ICommandStreamInfo deserialize() throws IOException
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
        
        var csInfo = new CommandStreamInfo.Builder()
            .withName(name)
            .withDescription(description)
            .withSystem(new SystemId(1, "temp-uid")) // use dummy UID since it will be replaced later
            .withRecordDescription(resultStruct)
            .withRecordEncoding(resultEncoding)
            .build();
        
        return csInfo;
    }


    @Override
    public void serialize(CommandStreamKey key, ICommandStreamInfo dsInfo, boolean showLinks) throws IOException
    {
        var publicDsID = encodeID(key.getInternalID());
        var publicSysID = sysIdEncoder.encodeID(dsInfo.getSystemID().getInternalID());
        
        writer.beginObject();
        
        writer.name("id").value(Long.toString(publicDsID, 36));
        writer.name("name").value(dsInfo.getName());
        
        if (dsInfo.getDescription() != null)
            writer.name("description").value(dsInfo.getDescription());
        
        writer.name("system").beginObject()
            .name("id").value(Long.toString(publicSysID, 36))
            .name("outputName").value(dsInfo.getControlInputName())
            .endObject();
        
        writer.name("validTime").beginArray()
            .value(dsInfo.getValidTime().begin().toString())
            .value(dsInfo.getValidTime().end().toString())
            .endArray();

        if (dsInfo.getExecutionTimeRange() != null)
        {
            writer.name("actuationTime").beginArray()
                .value(dsInfo.getExecutionTimeRange().begin().toString())
                .value(dsInfo.getExecutionTimeRange().end().toString())
                .endArray();
        }

        if (dsInfo.getIssueTimeRange() != null)
        {
            writer.name("issueTime").beginArray()
                .value(dsInfo.getIssueTimeRange().begin().toString())
                .value(dsInfo.getIssueTimeRange().end().toString())
                .endArray();
        }
        
        // observed properties
        writer.name("actuableProperties").beginArray();
        for (var obsProp: getActuables(dsInfo))
        {
            writer.beginObject();
            if (obsProp.getDefinition() != null)
                writer.name("definition").value(obsProp.getDefinition());
            if (obsProp.getLabel() != null)
                writer.name("label").value(obsProp.getLabel());
            if (obsProp.getDescription() != null)
                writer.name("description").value(obsProp.getDescription());
            writer.endObject();
        }
        writer.endArray();
        
        // available formats
        writer.name("formats").beginArray();
        writer.value(ResourceFormat.OM_JSON.getMimeType());
        if (ResourceFormat.allowNonBinaryFormat(dsInfo.getRecordEncoding()))
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
                      "/" + Long.toString(publicSysID, 36))
                .build());
            
            links.add(new ResourceLink.Builder()
                .rel("commands")
                .title("Collection of commands")
                .href(rootURL +
                      "/" + CommandStreamHandler.NAMES[0] +
                      "/" + Long.toString(publicDsID, 36) +
                      "/" + CommandHandler.NAMES[0])
                .build());
            
            writeLinksAsJson(writer, links);
        }
        
        writer.endObject();
        writer.flush();
    }
    
    
    protected Iterable<DataComponent> getActuables(ICommandStreamInfo dsInfo)
    {
        return Iterables.filter(new DataIterator(dsInfo.getRecordStructure()), comp -> {
            var def = comp.getDefinition();
            
            // skip vector coordinates
            if (comp.getParent() != null && comp.getParent() instanceof Vector)
                return false;
            
            // skip data records and choices
            if (comp instanceof DataRecord || comp instanceof DataChoice)
                return false;
            
            // skip well known fields
            if (SWEConstants.DEF_SAMPLING_TIME.equals(def) ||
                SWEConstants.DEF_PHENOMENON_TIME.equals(def) ||
                SWEConstants.DEF_SYSTEM_ID.equals(def))
                return false;
            
            // skip if no metadata was set
            if (Strings.isNullOrEmpty(def) &&
                Strings.isNullOrEmpty(comp.getLabel()) &&
                Strings.isNullOrEmpty(comp.getDescription()))
                return false;
            
            return true;
        });
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
