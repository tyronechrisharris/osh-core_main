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
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;
import com.google.gson.stream.JsonWriter;
import net.opengis.gml.v32.AbstractGeometry;


/**
 * <p>
 * GeoJSON formatter for feature resources
 * </p>
 *
 * @author Alex Robin
 * @since Jan 26, 2021
 */
public class FeatureBindingGeoJson extends AbstractFeatureBindingGeoJson<IGeoFeature>
{
    
    
    FeatureBindingGeoJson(ResourceContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder, forReading);
    }
    
    
    protected GeoJsonBindings getJsonBindings()
    {
        return new GeoJsonBindings() {
            protected void writeDateTimeValue(JsonWriter writer, OffsetDateTime dateTime) throws IOException
            {
                super.writeDateTimeValue(writer, dateTime.truncatedTo(ChronoUnit.SECONDS));
            }
        };
    }
    
    
    @Override
    protected IGeoFeature getFeatureWithId(FeatureKey key, IGeoFeature f)
    {
        Asserts.checkNotNull(key, FeatureKey.class);
        Asserts.checkNotNull(f, IGeoFeature.class);
        
        return new IGeoFeature() {
            
            @Override
            public String getId()
            {
                var externalID = FeatureBindingGeoJson.this.encodeID(key.getInternalID());
                return Long.toString(externalID, ResourceBinding.ID_RADIX);
            }

            @Override
            public String getUniqueIdentifier()
            {
                return f.getUniqueIdentifier();
            }

            @Override
            public String getName()
            {
                return f.getName();
            }

            @Override
            public String getDescription()
            {
                return f.getDescription();
            }

            @Override
            public AbstractGeometry getGeometry()
            {
                return f.getGeometry();
            }
        };
    }
}
