/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.property;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.property.PropertyKey;
import org.sensorhub.api.semantic.IDerivedProperty;
import org.sensorhub.impl.semantic.DerivedProperty;
import org.sensorhub.impl.service.consys.InvalidRequestException;
import org.sensorhub.impl.service.consys.ResourceParseException;
import org.sensorhub.impl.service.consys.procedure.ProcedureHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceBindingJson;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.resource.ResourceLink;
import org.sensorhub.impl.service.consys.system.SystemHandler;
import org.vast.swe.SWEJsonBindings;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


public class PropertyBindingJson extends ResourceBindingJson<PropertyKey, IDerivedProperty>
{
    final String rootURL;
    final SWEJsonBindings sweBindings;
    
    
    public PropertyBindingJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
        this.rootURL = ctx.getApiRootURL();
        this.sweBindings = new SWEJsonBindings();
    }
    
    
    @Override
    public IDerivedProperty deserialize(JsonReader reader) throws IOException
    {
        // if array, prepare to parse first element
        if (reader.peek() == JsonToken.BEGIN_ARRAY)
            reader.beginArray();
        
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
                
        var builder = new DerivedProperty.Builder();
        
        try
        {
            reader.beginObject();
            while (reader.hasNext())
            {
                var prop = reader.nextName();
                
                if ("uniqueId".equals(prop))
                    builder.uri(reader.nextString());
                else if ("label".equals(prop))
                    builder.name(reader.nextString());
                else if ("description".equals(prop))
                    builder.description(reader.nextString());
                else if ("baseProperty".equals(prop))
                    builder.baseProperty(reader.nextString());
                else if ("object".equals(prop))
                    builder.objectType(reader.nextString());
                else if ("statistic".equals(prop))
                    builder.statistic(reader.nextString());
                else if ("qualifiers".equals(prop))
                {
                    reader.beginArray();
                    while (reader.hasNext())
                    {
                        var comp = sweBindings.readDataComponent(reader);
                        builder.addQualifier(comp);
                    }
                    reader.endArray();
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
        
        return builder.build();
    }


    @Override
    public void serialize(PropertyKey key, IDerivedProperty prop, boolean showLinks, JsonWriter writer) throws IOException
    {
        var propId = idEncoders.getPropertyIdEncoder().encodeID(key.getInternalID());
        
        var conceptUri = prop.getURI();
        if (conceptUri.startsWith("#"))
            conceptUri = ctx.getApiRootURL() + "/" + PropertyHandler.NAMES[0] + "/" + conceptUri.substring(1);
        
        writer.beginObject();
        
        writer.name("id").value(propId);
        writer.name("uniqueId").value(conceptUri);
        writer.name("label").value(prop.getName());
        
        if (prop.getDescription() != null)
            writer.name("description").value(prop.getDescription());
        
        writer.name("baseProperty").value(prop.getBaseProperty());
        
        if (prop.getObjectType() != null)
            writer.name("object").value(prop.getObjectType());
        
        if (prop.getStatistic() != null)
            writer.name("statistic").value(prop.getStatistic());
        
        if (!prop.getQualifiers().isEmpty())
        {
            writer.name("qualifiers").beginArray();
            for (var q: prop.getQualifiers())
                sweBindings.writeDataComponent(writer, q, true);
            writer.endArray();
        }
        
        // links
        if (showLinks)
        {
            var links = new ArrayList<ResourceLink>();
            
            links.add(new ResourceLink.Builder()
                .rel("canonical")
                .href(rootURL +
                      "/" + PropertyHandler.NAMES[0] +
                      "/" + propId)
                .type(ResourceFormat.JSON.getMimeType())
                .build());
            
            links.add(new ResourceLink.Builder()
                .rel("systems")
                .title("Linked systems")
                .href(rootURL +
                      "/" + SystemHandler.NAMES[0] +
                      "?observedProperty=" + prop.getURI())
                .build());
            
            links.add(new ResourceLink.Builder()
                .rel("procedures")
                .title("Linked procedures")
                .href(rootURL +
                      "/" + ProcedureHandler.NAMES[0] +
                      "?observedProperty=" + prop.getURI())
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
