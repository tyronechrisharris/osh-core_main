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
import java.util.ArrayList;
import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureBindingGeoJson;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.obs.DataStreamHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * GeoJSON formatter for system resources
 * </p>
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class SystemBindingGeoJson extends AbstractFeatureBindingGeoJson<ISystemWithDesc>
{
    
    public SystemBindingGeoJson(RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
    }
    
    
    @Override
    protected GeoJsonBindings getJsonBindings()
    {
        return new GeoJsonBindings() {
            public IFeature readFeature(JsonReader reader) throws IOException
            {
                var f = super.readFeature(reader);
                return new SystemFeatureAdapter(f);
            }
            
            protected void writeCommonFeatureProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                super.writeCommonFeatureProperties(writer, bean);
            }
            
            protected void writeCustomJsonProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                if (showLinks.get())
                {
                    var links = new ArrayList<ResourceLink>();
                    
                    links.add(new ResourceLink.Builder()
                        .rel("details")
                        .title("Detailed system specsheet in SensorML format")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + SystemDetailsHandler.NAMES[0])
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("members")
                        .title("List of subsystems")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + SystemMembersHandler.NAMES[0])
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("datastreams")
                        .title("List of system datastreams")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + DataStreamHandler.NAMES[0])
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("fois")
                        .title("List of system features of interest")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + FoiHandler.NAMES[0])
                        .build());
                    
                    writeLinksAsJson(writer, links);
                }
            }
        };
    }
    
    
    @Override
    protected ISystemWithDesc getFeatureWithId(FeatureKey key, ISystemWithDesc proc)
    {
        Asserts.checkNotNull(key, FeatureKey.class);
        Asserts.checkNotNull(proc, ISystemWithDesc.class);
        
        return new ISystemWithDesc()
        {
            public String getUniqueIdentifier() { return proc.getUniqueIdentifier(); }
            public String getName() { return proc.getName(); }
            public String getDescription() { return proc.getDescription(); }
            public Map<QName, Object> getProperties() { return proc.getProperties(); }  
            public TimeExtent getValidTime() { return proc.getValidTime(); }
            public AbstractProcess getFullDescription() { return proc.getFullDescription(); }
        
            public String getId()
            {
                return idEncoders.getSystemIdEncoder().encodeID(key.getInternalID());
            }
        };
    }
}
