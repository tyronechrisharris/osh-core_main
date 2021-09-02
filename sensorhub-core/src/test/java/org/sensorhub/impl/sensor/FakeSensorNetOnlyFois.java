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
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IMultiSourceDataProducer;
import org.vast.ogc.gml.GenericFeatureImpl;
import org.vast.ogc.gml.IFeature;
import com.google.common.collect.ImmutableList;


public class FakeSensorNetOnlyFois extends FakeSensor implements IMultiSourceDataProducer
{
    public static final String SENSORNET_UID = "urn:sensors:mysensornet:001";
    public static final String FOI_UID_PREFIX = SENSORNET_UID + ":fois:";
    GMLFactory gmlFac = new GMLFactory(true);
    Map<String, AbstractFeature> fois = new LinkedHashMap<>();
    
    
    @Override
    protected void doInit() throws SensorHubException
    {
        super.doInit();
        this.uniqueID = SENSORNET_UID;
        this.xmlID = "SENSORNET1";
    }
    
    
    public void addFois(int numFois)
    {
        addFois(numFois, null);
    }
    
    
    public void addFois(int numFois, Consumer<IFeature> foiConfigurator)
    {
        for (int foiIdx = 1; foiIdx <= numFois; foiIdx++)
        {
            QName fType = new QName("http://myNsUri", "MyFeature");
            String foiUID = getFoiUID(foiIdx);
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
