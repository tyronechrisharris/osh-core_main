/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.consys.LinkResolver;
import org.sensorhub.impl.service.consys.feature.AbstractFeatureBindingGeoJson;
import org.sensorhub.impl.service.consys.feature.FoiHandler;
import org.sensorhub.impl.service.consys.obs.DataStreamHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.resource.ResourceLink;
import org.sensorhub.impl.service.consys.sensorml.SystemAdapter;
import org.sensorhub.impl.service.consys.task.CommandStreamHandler;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.xlink.IXlinkReference;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import net.opengis.gml.v32.AbstractGeometry;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * GeoJSON formatter for system resources
 * </p>
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class SystemBindingGeoJson extends AbstractFeatureBindingGeoJson<ISystemWithDesc, IObsSystemDatabase>
{
    
    public SystemBindingGeoJson(RequestContext ctx, IdEncoders idEncoders, IObsSystemDatabase db, boolean forReading) throws IOException
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
                return new SystemAdapter(f);
            }
            
            @Override
            public void writeLink(JsonWriter writer, IXlinkReference<?> link) throws IOException
            {
                LinkResolver.resolveLink(ctx, link, db, idEncoders);
                super.writeLink(writer, link);
            }
            
            @Override
            protected void writeCustomFeatureProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                super.writeCustomFeatureProperties(writer, bean);
                
                var sml = ((ISystemWithDesc)bean).getFullDescription();
                if (sml != null)
                {
                    var typeOf = sml.getTypeOf();
                    if (typeOf != null)
                    {
                        writer.name("systemKind@link");
                        writeLink(writer, typeOf);
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
                        .href("/" + SystemHandler.NAMES[0] + "/" + bean.getId())
                        .type(ResourceFormat.JSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("alternate")
                        .title("Detailed description of system in SensorML format")
                        .href("/" + SystemHandler.NAMES[0] + "/" + bean.getId() + "?f=" + ResourceFormat.SHORT_SMLJSON)
                        .type(ResourceFormat.SML_JSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("alternate")
                        .title("Detailed description of system in HTML format")
                        .href("/" + SystemHandler.NAMES[0] + "/" + bean.getId() + "?f=" + ResourceFormat.SHORT_HTML)
                        .type(ResourceFormat.HTML.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("subsystems")
                        .title("List of subsystems")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + SystemMembersHandler.NAMES[0] + "?f=" + ResourceFormat.SHORT_GEOJSON)
                        .type(ResourceFormat.GEOJSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("datastreams")
                        .title("List of system datastreams")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + DataStreamHandler.NAMES[0] + "?f=" + ResourceFormat.SHORT_JSON)
                        .type(ResourceFormat.JSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("controlstreams")
                        .title("List of system controlstreams")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + CommandStreamHandler.NAMES[0] + "?f=" + ResourceFormat.SHORT_JSON)
                        .type(ResourceFormat.JSON.getMimeType())
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("samplingFeatures")
                        .title("List of system sampling features")
                        .href("/" + SystemHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + FoiHandler.NAMES[0] + "?f=" + ResourceFormat.SHORT_GEOJSON)
                        .type(ResourceFormat.JSON.getMimeType())
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
            public String getType() { return proc.getType(); }
            public String getName() { return proc.getName(); }
            public String getDescription() { return proc.getDescription(); }
            public Map<QName, Object> getProperties() { return proc.getProperties(); }  
            public TimeExtent getValidTime() { return proc.getValidTime(); }
            public AbstractProcess getFullDescription() { return proc.getFullDescription(); }
            public AbstractGeometry getGeometry() { return proc.getGeometry(); }
        
            public String getId()
            {
                return idEncoders.getSystemIdEncoder().encodeID(key.getInternalID());
            }
        };
    }
}
