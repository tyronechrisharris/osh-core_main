/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.procedure;

import java.io.IOException;
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.system.wrapper.SmlFeatureWrapper;
import org.vast.sensorML.SMLStaxBindings;
import org.vast.sensorML.json.SMLJsonStreamReader;
import org.vast.sensorML.json.SMLJsonStreamWriter;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * SensorML JSON formatter for system resources
 * </p>
 * 
 * @param <V> Type of SML feature resource
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class SmlFeatureBindingSmlJson<V extends IProcedureWithDesc> extends ResourceBindingJson<FeatureKey, V>
{
    SMLJsonStreamReader smlReader;
    SMLJsonStreamWriter smlWriter;
    SMLStaxBindings smlBindings;
    
    
    public SmlFeatureBindingSmlJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
        this.smlBindings = new SMLStaxBindings();
        
        if (forReading)
            this.smlReader = new SMLJsonStreamReader(reader);
        else
            this.smlWriter = new SMLJsonStreamWriter(writer);
    }


    @Override
    public V deserialize(JsonReader reader) throws IOException
    {
        // if array, prepare to parse first element
        if (reader.peek() == JsonToken.BEGIN_ARRAY)
            reader.beginArray();
        
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
        
        try
        {
            smlReader.nextTag();
            var sml = smlBindings.readAbstractProcess(smlReader);
            
            @SuppressWarnings("unchecked")
            var wrapper = (V)new SmlFeatureWrapper(sml);
            return wrapper;
        }
        catch (JsonParseException | XMLStreamException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage(), e);
        }
    }


    @Override
    public void serialize(FeatureKey key, V res, boolean showLinks, JsonWriter writer) throws IOException
    {
        try
        {
            try
            {
                smlWriter.resetContext();
                var sml = res.getFullDescription();
                if (sml != null)
                    smlBindings.writeAbstractProcess(smlWriter, sml);
                smlWriter.flush();
            }
            catch (Exception e)
            {
                IOException wrappedEx = new IOException("Error writing system JSON", e);
                throw new IllegalStateException(wrappedEx);
            }
        }
        catch (IllegalStateException e)
        {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            else
                throw e;
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
}
