/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.system;

import java.io.IOException;
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingXml;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.system.wrapper.SystemWrapper;
import org.vast.sensorML.SMLStaxBindings;


/**
 * <p>
 * SensorML XML formatter for system resources
 * </p>
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class SystemBindingSmlXml extends ResourceBindingXml<FeatureKey, ISystemWithDesc>
{
    SMLStaxBindings smlBindings;
    
    
    public SystemBindingSmlXml(RequestContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder, forReading);
        
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
    public ISystemWithDesc deserialize() throws IOException
    {
        try
        {
            if (!xmlReader.hasNext())
                return null;
            
            xmlReader.nextTag();
            var sml = smlBindings.readAbstractProcess(xmlReader);
            return new SystemWrapper(sml);
        }
        catch (XMLStreamException e)
        {
            throw new ResourceParseException(INVALID_XML_ERROR_MSG + e.getMessage(), e);
        }
    }


    @Override
    public void serialize(FeatureKey key, ISystemWithDesc res, boolean showLinks) throws IOException
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
