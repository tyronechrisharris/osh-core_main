/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.deployment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.IDeploymentWithDesc;
import org.sensorhub.impl.service.consys.LinkResolver;
import org.sensorhub.impl.service.consys.feature.AbstractFeatureBindingGeoJson;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.resource.ResourceLink;
import org.sensorhub.impl.service.consys.sensorml.DeploymentAdapter;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.xlink.IXlinkReference;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import net.opengis.gml.v32.AbstractGeometry;
import net.opengis.sensorml.v20.Deployment;


/**
 * <p>
 * GeoJSON formatter for deployment resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2023
 */
public class DeploymentBindingGeoJson extends AbstractFeatureBindingGeoJson<IDeploymentWithDesc, IObsSystemDatabase>
{
    
    public DeploymentBindingGeoJson(RequestContext ctx, IdEncoders idEncoders, IObsSystemDatabase db, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, db, forReading);
    }
    
    
    @Override
    protected GeoJsonBindings getJsonBindings()
    {
        return new GeoJsonBindings() {
            
            @Override
            public IFeature readFeature(JsonReader reader) throws IOException
            {
                var f = super.readFeature(reader);
                return new DeploymentAdapter(f);
            }
            
            @Override
            public void writeLink(JsonWriter writer, IXlinkReference<?> link) throws IOException
            {
                LinkResolver.resolveLink(ctx, link, db, idEncoders);
                super.writeLink(writer, link);
            }
            
            @Override
            protected void writeCommonFeatureProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                super.writeCommonFeatureProperties(writer, bean);
                
                var sml = ((IDeploymentWithDesc)bean).getFullDescription();
                if (sml != null)
                {
                    var platform = sml.getPlatform();
                    if (platform != null)
                    {
                        writer.name("platform@link");
                        writeLink(writer, platform.getSystemRef());
                    }
                    
                    if (sml.getNumDeployedSystems() > 0)
                    {
                        writer.name("deployedSystems@link").beginArray();
                        var systems = sml.getDeployedSystemList();
                        for (var sys: systems)
                            writeLink(writer, sys.getSystemRef());
                        writer.endArray();
                    }
                }
            }
            
            @Override
            protected void writeCustomJsonProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                super.writeCustomJsonProperties(writer, bean);
                
                if (showLinks.get())
                {
                    var links = new ArrayList<ResourceLink>();
                    
                    links.add(new ResourceLink.Builder()
                        .rel("canonical")
                        .href("/" + DeploymentHandler.NAMES[0] +
                              "/" + bean.getId())
                        .type(ResourceFormat.JSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("alternate")
                        .title("Detailed description of deployment in SensorML format")
                        .href("/" + DeploymentHandler.NAMES[0] + "/" + bean.getId() + "?f=" + ResourceFormat.SHORT_SMLJSON)
                        .type(ResourceFormat.SML_JSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("alternate")
                        .title("Detailed description of deployment in HTML format")
                        .href("/" + DeploymentHandler.NAMES[0] + "/" + bean.getId() + "?f=" + ResourceFormat.SHORT_HTML)
                        .type(ResourceFormat.HTML.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("subdeployments")
                        .title("List of subdeployments")
                        .href("/" + DeploymentHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + DeploymentMembersHandler.NAMES[0] + "?f=" + ResourceFormat.SHORT_GEOJSON)
                        .type(ResourceFormat.GEOJSON.getMimeType())
                        .build());
                    
                    writeLinksAsJson(writer, links);
                }
            }
        };
    }
    
    
    @Override
    protected IDeploymentWithDesc getFeatureWithId(FeatureKey key, IDeploymentWithDesc depl)
    {
        Asserts.checkNotNull(key, FeatureKey.class);
        Asserts.checkNotNull(depl, IDeploymentWithDesc.class);
        
        return new IDeploymentWithDesc()
        {
            public String getUniqueIdentifier() { return depl.getUniqueIdentifier(); }
            public String getType() { return depl.getType(); }
            public String getName() { return depl.getName(); }
            public String getDescription() { return depl.getDescription(); }
            public Map<QName, Object> getProperties() { return depl.getProperties(); }  
            public TimeExtent getValidTime() { return depl.getValidTime(); }
            public Deployment getFullDescription() { return depl.getFullDescription(); }
            public AbstractGeometry getGeometry() { return depl.getGeometry(); }
            
            public String getId()
            {
                return idEncoders.getDeploymentIdEncoder().encodeID(key.getInternalID());
            }
        };
    }
}
