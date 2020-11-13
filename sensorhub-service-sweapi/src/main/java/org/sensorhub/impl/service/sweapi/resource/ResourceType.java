/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.sensorhub.impl.service.sweapi.IdUtils;
import org.sensorhub.impl.service.sweapi.json.FilteredJsonWriter;
import com.google.gson.stream.JsonWriter;


public abstract class ResourceType<K, V>
{
    public static final int ID_RADIX = 36;
    public static final String INDENT = "  ";
    
    protected final IdUtils idUtils;
    
    
    protected ResourceType(IdUtils idUtils)
    {
        this.idUtils = idUtils;
    }
    
    
    public abstract V deserialize(ResourceFormat format, InputStream is) throws IOException;
    public abstract Iterator<? extends V> deserializeArray(ResourceFormat format, InputStream is) throws IOException;
    public abstract void serialize(K key, V res, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException;
    public abstract void serialize(Stream<Entry<K, V>> results, Collection<ResourceLink> links, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException;
    
    
    public long getInternalID(long externalID)
    {
        return idUtils.getInternalID(externalID);
    }
    
    
    public long getExternalID(long internalID)
    {
        return idUtils.getExternalID(internalID);
    }
    
    
    protected JsonWriter getJsonWriter(OutputStream os, PropertyFilter propFilter) throws IOException
    {
        JsonWriter writer;
        var osw = new OutputStreamWriter(os, StandardCharsets.UTF_8.name());        
        if (propFilter != null)
            writer = new FilteredJsonWriter(osw, propFilter);
        else
            writer = new JsonWriter(osw);
        
        writer.setLenient(true);
        writer.setSerializeNulls(false);
        writer.setIndent(INDENT);
        return writer;
    }
    
    
    protected void writeLinksAsJson(Collection<ResourceLink> links, JsonWriter writer) throws IOException
    {
        writer.beginArray();
        
        for (var l: links)
        {
            writer.beginObject();
            writer.name("rel").value(l.getRel());
            if (l.getTitle() != null)
                writer.name("title").value(l.getTitle());
            writer.name("href").value(l.getHref());
            writer.name("type").value(l.getType());
            writer.endObject();
        }
        
        writer.endArray();
    }
}