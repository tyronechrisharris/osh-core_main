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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.impl.service.sweapi.IdUtils;
import org.sensorhub.impl.service.sweapi.StreamException;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IFeature;
import com.google.gson.stream.JsonReader;


public abstract class AbstractFeatureResourceType<V extends IFeature> extends ResourceType<FeatureKey, V>
{
    
    public AbstractFeatureResourceType(IdUtils idUtils)
    {
        super(idUtils);
    }
    
    
    /*
     * To be implemented by subclasses to wrap feature to assign internal ID
     */
    protected abstract V getFeatureWithId(FeatureKey k, V f);
        
    
    protected GeoJsonBindings getJsonBindings()
    {
        return new GeoJsonBindings(true);
    }
    
    
    protected JsonReader getJsonReader(InputStream is) throws IOException
    {
        JsonReader reader = new JsonReader(new InputStreamReader(is, StandardCharsets.UTF_8.name()));
        reader.setLenient(true);
        return reader;
    }


    @Override
    @SuppressWarnings("unchecked")
    public V deserialize(ResourceFormat format, InputStream is) throws IOException
    {
        JsonReader reader = getJsonReader(is);
        IFeature f = getJsonBindings().readFeature(reader);
        return (V)f;
    }


    @Override
    @SuppressWarnings("unchecked")
    public Iterator<V> deserializeArray(ResourceFormat format, InputStream is) throws IOException
    {
        final JsonReader reader = getJsonReader(is);
        final GeoJsonBindings geoJsonBindings = getJsonBindings();
        
        reader.beginArray();        
        return new Iterator<V>() {
            
            @Override
            public boolean hasNext()
            {
                try { return reader.hasNext(); }
                catch (IOException e) { return false; }
            }

            @Override
            public V next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();
                
                try
                {
                    IFeature f = geoJsonBindings.readFeature(reader);
                    return (V)f;
                }
                catch (Exception e)
                {
                    throw new StreamException(e);
                }
            }                    
        };
    }


    @Override
    public void serialize(FeatureKey k, V f, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        try
        {
            var writer = getJsonWriter(os, propFilter);
            getJsonBindings().writeFeature(writer, getFeatureWithId(k, f));
            writer.flush();
        }
        catch (IOException e)
        {
            throw new IOException("Error writing feature JSON", e);
        }
    }


    @Override
    public void serialize(Stream<Entry<FeatureKey, V>> results, Collection<ResourceLink> links, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        var geoJsonBindings = getJsonBindings();
        
        writer.beginArray();
        
        try
        {
            results.forEach(entry -> {
                try
                {
                    var f = getFeatureWithId(entry.getKey(), entry.getValue());
                    geoJsonBindings.writeFeature(writer, f);
                }
                catch (IOException e)
                {
                    IOException wrappedEx = new IOException("Error writing feature JSON", e);
                    throw new IllegalStateException(wrappedEx);
                }
            });
        }
        catch (IllegalStateException e)
        {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            else
                throw e;
        }
        
        writer.endArray();
        writer.flush();
    }
}
