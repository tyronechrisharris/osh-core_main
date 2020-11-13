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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureWrapper;
import org.sensorhub.impl.service.sweapi.IdUtils;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.vast.json.JsonStreamException;
import org.vast.sensorML.SMLStaxBindings;
import org.vast.sensorML.json.SMLJsonStreamReader;
import org.vast.sensorML.json.SMLJsonStreamWriter;
import net.opengis.sensorml.v20.AbstractProcess;


public class ProcedureDetailsResourceType extends ResourceType<FeatureKey, IProcedureWithDesc>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    SMLStaxBindings smlBindings;
    
    
    ProcedureDetailsResourceType()
    {
        super(new IdUtils(EXTERNAL_ID_SEED));
        this.smlBindings = new SMLStaxBindings();
    }
    

    @Override
    public IProcedureWithDesc deserialize(ResourceFormat format, InputStream is) throws IOException
    {
        try
        {
            SMLJsonStreamReader reader = new SMLJsonStreamReader(is, StandardCharsets.UTF_8.name());
            return readProcedure(reader);
        }
        catch (Exception e)
        {
            throw new IOException("Error reading procedure JSON", e);
        }
    }
    
    
    protected IProcedureWithDesc readProcedure(SMLJsonStreamReader reader) throws XMLStreamException
    {
        var sml = smlBindings.readAbstractProcess(reader);
        return new ProcedureWrapper(sml);
    }


    @Override
    public Iterator<? extends IProcedureWithDesc> deserializeArray(ResourceFormat format, InputStream is) throws IOException
    {
        return null;
    }


    @Override
    public void serialize(FeatureKey key, IProcedureWithDesc res, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        
        try
        {
            var sml = res.getFullDescription();
            if (sml != null)
            {
                SMLJsonStreamWriter streamWriter = new SMLJsonStreamWriter(writer);
                smlBindings.writeAbstractProcess(streamWriter, withInternalProcessId(key, sml));
            }
        }
        catch (Exception e)
        {
            throw new IOException("Error writing procedure JSON", e);
        }
    }


    @Override
    public void serialize(Stream<Entry<FeatureKey, IProcedureWithDesc>> results, Collection<ResourceLink> links, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        
        try
        {
            final SMLJsonStreamWriter streamWriter = new SMLJsonStreamWriter(writer);
            streamWriter.beginArray();
            
            AtomicBoolean first = new AtomicBoolean(true);
            results.forEach(entry -> {
                try
                {
                    if (!first.getAndSet(false))
                        writer.flush();
                    streamWriter.nextArrayElt();
                    
                    var sml = entry.getValue().getFullDescription();
                    if (sml != null)
                        smlBindings.writeAbstractProcess(streamWriter, withInternalProcessId(entry.getKey(), sml));
                }
                catch (Exception e)
                {
                    IOException wrappedEx = new IOException("Error writing procedure JSON", e);
                    throw new IllegalStateException(wrappedEx);
                }
            });
            
            streamWriter.endArray();
            streamWriter.flush();
        }
        catch (IllegalStateException e)
        {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            else
                throw e;
        }
        catch (JsonStreamException e)
        {
            throw new IOException(e.getMessage());
        } 
    }
    
    
    protected AbstractProcess withInternalProcessId(FeatureKey key, AbstractProcess sml)
    {
        var externalID = getExternalID(key.getInternalID());
        sml.setId(Long.toString(externalID, ResourceType.ID_RADIX));
        return sml;
    }
}
