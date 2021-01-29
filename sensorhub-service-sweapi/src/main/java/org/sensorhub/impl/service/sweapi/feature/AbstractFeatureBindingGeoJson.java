/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.feature;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IFeature;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * Base class for all GeoJSON feature formatter.
 * </p>
 * 
 * @param <V> Feature type
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public abstract class AbstractFeatureBindingGeoJson<V extends IFeature> extends ResourceBindingJson<FeatureKey, V>
{
    JsonReader reader;
    JsonWriter writer;
    GeoJsonBindings geoJsonBindings;
    
    
    public AbstractFeatureBindingGeoJson(ResourceContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder);
        
        this.geoJsonBindings = getJsonBindings(false);
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getRequest().getInputStream());
            this.reader = getJsonReader(is);
        }
        else
        {
            this.writer = getJsonWriter(ctx.getResponse().getOutputStream(), ctx.getPropertyFilter());
        }
    }
    
    
    /*
     * To be implemented by subclasses to wrap feature to assign internal ID
     */
    protected abstract V getFeatureWithId(FeatureKey k, V f);


    @Override
    @SuppressWarnings("unchecked")
    public V deserialize() throws IOException
    {
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
        
        var geoJsonBindings = getJsonBindings(false);
        return (V)geoJsonBindings.readFeature(reader);
    }


    @Override
    public void serialize(FeatureKey key, V res, boolean showLinks) throws IOException
    {
        try
        {
            var f = getFeatureWithId(key, res);
            geoJsonBindings.writeFeature(writer, f);
            writer.flush();
        }
        catch (IOException e)
        {
            IOException wrappedEx = new IOException("Error writing feature JSON", e);
            throw new IllegalStateException(wrappedEx);
        }
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
    
    
    @Override
    protected JsonReader getJsonReader(InputStream is) throws IOException
    {
        JsonReader reader = new JsonReader(new InputStreamReader(is, StandardCharsets.UTF_8.name()));
        reader.setLenient(true);
        return reader;
    }
    
    
    protected GeoJsonBindings getJsonBindings(boolean showLinks)
    {
        return new GeoJsonBindings(true);
    }
}
