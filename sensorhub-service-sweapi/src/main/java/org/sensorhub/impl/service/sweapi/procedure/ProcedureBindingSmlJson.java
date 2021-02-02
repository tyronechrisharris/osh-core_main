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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.procedure.wrapper.ProcedureWrapper;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.vast.sensorML.SMLStaxBindings;
import org.vast.sensorML.json.SMLJsonStreamReader;
import org.vast.sensorML.json.SMLJsonStreamWriter;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


/**
 * <p>
 * SensorML JSON formatter for procedure resources
 * </p>
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class ProcedureBindingSmlJson extends ResourceBindingJson<FeatureKey, IProcedureWithDesc>
{
    JsonReader reader;
    SMLJsonStreamReader smlReader;
    JsonWriter writer;
    SMLJsonStreamWriter smlWriter;
    SMLStaxBindings smlBindings;
    
    
    ProcedureBindingSmlJson(ResourceContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder);
        this.smlBindings = new SMLStaxBindings();
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getRequest().getInputStream());
            this.reader = getJsonReader(is);
            this.smlReader = new SMLJsonStreamReader(reader);
        }
        else
        {
            this.writer = getJsonWriter(ctx.getResponse().getOutputStream(), ctx.getPropertyFilter());
            this.smlWriter = new SMLJsonStreamWriter(writer);
        }
    }


    @Override
    public IProcedureWithDesc deserialize() throws IOException
    {
        if (reader.peek() == JsonToken.END_DOCUMENT || !reader.hasNext())
            return null;
        
        try
        {
            smlReader.nextTag();
            var sml = smlBindings.readAbstractProcess(smlReader);
            return new ProcedureWrapper(sml);
        }
        catch (JsonParseException | XMLStreamException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage(), e);
        }
    }


    @Override
    public void serialize(FeatureKey key, IProcedureWithDesc res, boolean showLinks) throws IOException
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
                IOException wrappedEx = new IOException("Error writing procedure JSON", e);
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
