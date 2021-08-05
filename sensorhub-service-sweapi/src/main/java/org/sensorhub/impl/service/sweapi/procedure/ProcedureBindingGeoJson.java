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
import java.util.ArrayList;
import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureBindingGeoJson;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.obs.DataStreamHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.ogc.om.IProcedure;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * GeoJSON formatter for procedure resources
 * </p>
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class ProcedureBindingGeoJson extends AbstractFeatureBindingGeoJson<IProcedureWithDesc>
{
    
    ProcedureBindingGeoJson(ResourceContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder, forReading);
    }
    
    
    @Override
    protected GeoJsonBindings getJsonBindings()
    {
        return new GeoJsonBindings() {
            public IFeature readFeature(JsonReader reader) throws IOException
            {
                var f = (ITemporalFeature)super.readFeature(reader);
                return new ProcedureFeatureAdapter(f);
            }
            
            protected void writeCommonFeatureProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                if (bean.getType() != null)
                    writer.name("definition").value(bean.getType());
                super.writeCommonFeatureProperties(writer, bean);
            }
            
            protected void writeCustomJsonProperties(JsonWriter writer, IFeature bean) throws IOException
            {
                if (showLinks.get())
                {
                    var links = new ArrayList<ResourceLink>();
                    
                    links.add(new ResourceLink.Builder()
                        .rel("details")
                        .title("Detailed procedure specsheet in SensorML format")
                        .href("/" + ProcedureHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + ProcedureDetailsHandler.NAMES[0])
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("members")
                        .title("List of procedure group members")
                        .href("/" + ProcedureHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + ProcedureMembersHandler.NAMES[0])
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("datastreams")
                        .title("List of procedure datastreams")
                        .href("/" + ProcedureHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + DataStreamHandler.NAMES[0])
                        .build());
                    
                    links.add(new ResourceLink.Builder()
                        .rel("fois")
                        .title("List of procedure features of interest")
                        .href("/" + ProcedureHandler.NAMES[0] + "/" +
                            bean.getId() + "/" + FoiHandler.NAMES[0])
                        .build());
                    
                    writeLinksAsJson(writer, links);
                }
            }
        };
    }
    
    
    @Override
    protected IProcedureWithDesc getFeatureWithId(FeatureKey key, IProcedureWithDesc proc)
    {
        Asserts.checkNotNull(key, FeatureKey.class);
        Asserts.checkNotNull(proc, IProcedure.class);
        
        return new IProcedureWithDesc()
        {
            public String getUniqueIdentifier() { return proc.getUniqueIdentifier(); }
            public String getName() { return proc.getName(); }
            public String getDescription() { return proc.getDescription(); }
            public Map<QName, Object> getProperties() { return proc.getProperties(); }  
            public TimeExtent getValidTime() { return proc.getValidTime(); }
            public AbstractProcess getFullDescription() { return proc.getFullDescription(); }
        
            public String getId()
            {
                var externalID = ProcedureBindingGeoJson.this.encodeID(key.getInternalID());
                return Long.toString(externalID, ResourceBinding.ID_RADIX);
            }
        };
    }
}
