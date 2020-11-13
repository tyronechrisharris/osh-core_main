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
import org.sensorhub.impl.service.sweapi.IdUtils;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;
import com.google.gson.stream.JsonWriter;
import net.opengis.gml.v32.AbstractGeometry;


public class FeatureResourceType extends AbstractFeatureResourceType<IGeoFeature>
{
    public static final int EXTERNAL_ID_SEED = 815420;
    
    
    FeatureResourceType()
    {
        super(new IdUtils(EXTERNAL_ID_SEED));
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
                var externalID = FeatureResourceType.this.getExternalID(key.getInternalID());
                return Long.toString(externalID, ResourceType.ID_RADIX);
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
