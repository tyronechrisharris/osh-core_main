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
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.feature.ISmlFeature;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingXml;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.system.wrapper.SmlFeatureWrapper;
import org.vast.sensorML.SMLStaxBindings;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * SensorML XML formatter for system resources
 * </p>
 * 
 * @param <V> Type of SML feature resource
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class SmlFeatureBindingSmlXml<V extends ISmlFeature<AbstractProcess>> extends ResourceBindingXml<FeatureKey, V>
{
    SMLStaxBindings smlBindings;
    
    
    public SmlFeatureBindingSmlXml(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
        
        try
        {
            this.smlBindings = new SMLStaxBindings();
            if (!forReading)
            {
                smlBindings.setNamespacePrefixes(xmlWriter);
                smlBindings.declareNamespacesOnRootElement();
            }
        }
        catch (XMLStreamException e)
        {
            throw new IOException("Error initializing XML bindings", e);
        }
    }


    @Override
    public V deserialize() throws IOException
    {
        try
        {
            // skip all events until we reach the next XML element or abort.
            // This is needed because deserialize() is called multiple times and nextTag()
            // breaks if there is no more non-whitespace content.
            while (xmlReader.getEventType() != XMLStreamConstants.START_ELEMENT)
            {                
                if (!xmlReader.hasNext())
                    return null;
                else
                    xmlReader.next();
            }
            
            var sml = smlBindings.readAbstractProcess(xmlReader);
            
            @SuppressWarnings("unchecked")
            var wrapper = (V)new SmlFeatureWrapper(sml);
            return wrapper;
        }
        catch (XMLStreamException e)
        {
            throw new ResourceParseException(INVALID_XML_ERROR_MSG + e.getMessage(), e);
        }
    }


    @Override
    public void serialize(FeatureKey key, V res, boolean showLinks) throws IOException
    {
        try
        {
            try
            {
                var sml = res.getFullDescription();
                if (sml != null)
                    smlBindings.writeAbstractProcess(xmlWriter, sml);
                xmlWriter.flush();
            }
            catch (Exception e)
            {
                IOException wrappedEx = new IOException("Error writing system XML", e);
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
        try
        {
            xmlWriter.writeStartElement("systems");
        }
        catch (XMLStreamException e)
        {
            throw new IOException(e);
        }
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        try
        {
            xmlWriter.writeEndElement();
        }
        catch (XMLStreamException e)
        {
            throw new IOException(e);
        }
    }
}
