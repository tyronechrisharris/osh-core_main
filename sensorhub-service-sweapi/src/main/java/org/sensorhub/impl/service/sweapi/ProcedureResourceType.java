/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.om.IProcedure;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;


public class ProcedureResourceType extends AbstractFeatureResourceType<IProcedureWithDesc>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    
    
    ProcedureResourceType()
    {
        super(new IdUtils(EXTERNAL_ID_SEED));
    }
    
    
    protected GeoJsonBindings getJsonBindings()
    {
        return new GeoJsonBindings() {
            /*protected void writeDateTimeValue(JsonWriter writer, OffsetDateTime dateTime) throws IOException
            {
                super.writeDateTimeValue(writer, dateTime.truncatedTo(ChronoUnit.SECONDS));
            }*/
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
                var externalID = ProcedureResourceType.this.getExternalID(key.getInternalID());
                return Long.toString(externalID, ResourceType.ID_RADIX);
            }
        };
    }
}
