/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.resource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.impl.service.consys.ServiceErrors;
import org.sensorhub.impl.service.consys.json.FilteredJsonWriter;
import org.vast.json.JsonInliningWriter;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * Base class for all JSON resource formatters
 * </p>
 * 
 * @param <K> Resource Key
 * @param <V> Resource Object
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public abstract class ResourceBindingJson<K, V> extends ResourceBinding<K, V>
{
    public static final String INVALID_JSON_ERROR_MSG = "Invalid JSON: ";
    public static final String MISSING_PROP_ERROR_MSG = "Missing property: ";
    protected final JsonReader reader;
    protected final JsonWriter writer;
    
    
    protected ResourceBindingJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders);
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getInputStream());
            this.reader = getJsonReader(is);
            this.writer = null;
        }
        else
        {
            this.writer = getJsonWriter(ctx.getOutputStream(), ctx.getPropertyFilter());
            this.reader = null;
        }
    }
    
    
    /* constructor used when reading a nested resource */
    protected ResourceBindingJson(RequestContext ctx, IdEncoders idEncoders, JsonReader reader) throws IOException
    {
        super(ctx, idEncoders);
        this.reader = reader;
        this.writer = null;
    }
    
    
    public abstract V deserialize(JsonReader reader) throws IOException;
    public abstract void serialize(K key, V res, boolean showLinks, JsonWriter writer) throws IOException;
    
    
    @Override
    public V deserialize() throws IOException
    {
        try
        {
            return deserialize(this.reader);
        }
        catch (JsonParseException e)
        {
            throw ServiceErrors.invalidPayload(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
    }
    
    
    @Override
    public void serialize(K key, V res, boolean showLinks) throws IOException
    {
        serialize(key, res, showLinks, this.writer);
    }
    
    
    protected JsonReader getJsonReader(InputStream is) throws IOException
    {
        var osr = new InputStreamReader(is, StandardCharsets.UTF_8);
        return new JsonReader(osr);
    }
    
    
    protected JsonWriter getJsonWriter(OutputStream os, PropertyFilter propFilter) throws IOException
    {
        JsonWriter writer;
        var osw = new OutputStreamWriter(os, StandardCharsets.UTF_8);
        if (propFilter != null)
            writer = new FilteredJsonWriter(osw, propFilter);
        else
            writer = new JsonInliningWriter(osw);
        
        writer.setLenient(true);
        writer.setSerializeNulls(false);
        writer.setIndent(INDENT);
        return writer;
    }
    
    
    protected void startJsonCollection(JsonWriter writer) throws IOException
    {
        writer.beginObject();
        writer.name(getItemsPropertyName());
        writer.beginArray();
    }
    
    
    protected void endJsonCollection(JsonWriter writer, Collection<ResourceLink> links) throws IOException
    {
        writer.endArray(); // end items list
        writeLinksAsJson(writer, links);
        writer.endObject();
        writer.flush();
    }
    
    
    protected void writeLinksAsJson(JsonWriter writer, Collection<ResourceLink> links) throws IOException
    {
        if (links != null && !links.isEmpty())
        {
            writer.name("links").beginArray();
            for (var l: links)
            {
                writer.beginObject();
                writer.name("rel").value(l.getRel());
                if (l.getTitle() != null)
                    writer.name("title").value(l.getTitle());
                writer.name("href").value(getAbsoluteHref(l.getHref()));
                if (l.getType() != null)
                    writer.name("type").value(l.getType());
                
                writer.endObject();
            }        
            writer.endArray();
        }
    }
    
    
    protected String getItemsPropertyName()
    {
        return "items";
    }
}