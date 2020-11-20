/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.sensor;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import javax.xml.namespace.QName;
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.gml.v32.Point;
import net.opengis.gml.v32.impl.GMLFactory;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IMultiSourceDataProducer;
import org.vast.ogc.gml.GenericFeature;
import org.vast.ogc.gml.GenericFeatureImpl;
import com.google.common.collect.ImmutableList;


public class FakeSensorNetOnlyFois extends FakeSensor implements IMultiSourceDataProducer
{
    static String SENSORNET_UID = "urn:mysensornet-001";
    static String FOI_UID_PREFIX = SENSORNET_UID + ":fois:";
    GMLFactory gmlFac = new GMLFactory(true);
    Map<String, AbstractFeature> fois = new LinkedHashMap<>();
    
    
    public FakeSensorNetOnlyFois()
    {
        this.uniqueID = SENSORNET_UID;
        this.xmlID = "SENSORNET";
    }


    @Override
    public void init()
    {
    }
    
    
    public void addFois(int numFois)
    {
        addFois(numFois, null);
    }
    
    
    public void addFois(int numFois, Consumer<GenericFeature> foiConfigurator)
    {
        for (int foiIdx = 1; foiIdx <= numFois; foiIdx++)
        {
            QName fType = new QName("http://myNsUri", "MyFeature");
            String foiUID = FOI_UID_PREFIX + foiIdx;
            var foi = new GenericFeatureImpl(fType);
            foi.setId("F" + foiIdx);
            foi.setUniqueIdentifier(foiUID);
            foi.setName("FOI" + foiIdx);
            foi.setDescription("This is feature of interest #" + foiIdx);
            Point p = gmlFac.newPoint();
            p.setPos(new double[] {foiIdx, foiIdx, 0.0});
            foi.setGeometry(p);
            if (foiConfigurator != null)
                foiConfigurator.accept(foi);
            fois.put(foiUID, foi);
        }
    }


    @Override
    public Map<String, ? extends IDataProducer> getMembers()
    {
        return Collections.emptyMap();
    }


    @Override
    public Map<String, AbstractFeature> getCurrentFeaturesOfInterest()
    {
        return Collections.unmodifiableMap(fois);
    }
    
    
    @Override
    public String getFoiUID(int foiNum)
    {
        return String.format(FOI_UID_PREFIX + "%03d", foiNum);
    }


    @Override
    public Collection<String> getProceduresWithFoi(String foiID)
    {
        if (fois.containsKey(foiID))
            return ImmutableList.of(getUniqueIdentifier());
        else
            return Collections.emptyList();
    }
    
}
