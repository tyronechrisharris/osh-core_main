/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import javax.xml.namespace.QName;
import net.opengis.fes.v20.BBOX;
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.swe.v20.DataArray;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataRecord;
import net.opengis.swe.v20.SimpleComponent;
import net.opengis.swe.v20.Vector;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IMultiSourceDataProducer;
import org.sensorhub.api.persistence.IFoiFilter;
import org.sensorhub.impl.persistence.StorageUtils;
import org.sensorhub.impl.persistence.FilteredIterator;
import org.vast.ogc.def.DefinitionRef;
import org.vast.ogc.gml.FeatureRef;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.om.IObservation;
import org.vast.ogc.om.ObservationImpl;
import org.vast.ogc.om.ProcedureRef;
import org.vast.ows.OWSRequest;
import org.vast.ows.fes.FESRequestUtils;
import org.vast.swe.SWEConstants;
import org.vast.util.Bbox;
import org.vast.util.TimeExtent;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;


public class SOSProviderUtils
{
    private static final QName EXT_REPLAY = new QName("replayspeed"); // kvp params are always lower case
    
    
    private SOSProviderUtils() {}
    
    
    public static double getReplaySpeed(OWSRequest request)
    {
        if (request.getExtensions().containsKey(EXT_REPLAY))
        {
            String replaySpeed = (String)request.getExtensions().get(EXT_REPLAY);
            return Double.parseDouble(replaySpeed);
        }
        
        return 1.0;
    }
    
    
    public static Geometry toRoiGeometry(BBOX spatialFilter)
    {
        Bbox bbox = FESRequestUtils.filterToBbox(spatialFilter);
        return new GeometryFactory().createPolygon(new Coordinate[] {
           new Coordinate(bbox.getMinX(), bbox.getMinY()),
           new Coordinate(bbox.getMinX(), bbox.getMaxY()),
           new Coordinate(bbox.getMaxX(), bbox.getMaxY()),
           new Coordinate(bbox.getMaxX(), bbox.getMinY()),
           new Coordinate(bbox.getMinX(), bbox.getMinY())
        });
    }
    
    
    @SuppressWarnings("unchecked")
    public static Iterator<AbstractFeature> getFilteredFoiIterator(IDataProducer producer, final IFoiFilter filter)
    {
        // get all fois from producer
        Iterator<? extends IGeoFeature> allFois;
        if (producer instanceof IMultiSourceDataProducer)
            allFois = ((IMultiSourceDataProducer)producer).getFeaturesOfInterest().values().iterator();
        else if (producer.getCurrentFeatureOfInterest() != null)
            allFois = Arrays.asList(producer.getCurrentFeatureOfInterest()).iterator();
        else
            allFois = Collections.emptyIterator();
        
        // return all features if no filter is used
        if ((filter.getFeatureIDs() == null || filter.getFeatureIDs().isEmpty()) && filter.getRoi() == null)
            return (Iterator<AbstractFeature>)allFois;
        
        return new FilteredIterator<AbstractFeature>((Iterator<AbstractFeature>)allFois)
        {
            @Override
            protected boolean accept(AbstractFeature f)
        {        
                if (StorageUtils.isFeatureSelected(filter, f))
                    return true;
                else
                    return false;
        }
        };
    }
    
    
    public static String getObsType(DataComponent result)
    {
        String obsType;
        if (result instanceof SimpleComponent)
            obsType = IObservation.OBS_TYPE_SCALAR;
        else if (result instanceof DataRecord || result instanceof Vector)
            obsType = IObservation.OBS_TYPE_RECORD;
        else if (result instanceof DataArray)
            obsType = IObservation.OBS_TYPE_ARRAY;
        else
            throw new IllegalStateException("Unsupported obs type");
        return obsType;
    }
    
    
    public static IObservation buildObservation(String procURI, String foiURI, DataComponent result)
    {        
        // get phenomenon time from record 'SamplingTime' if present
        // otherwise use current time
        double samplingTime = System.currentTimeMillis() / 1000.;
        for (int i = 0; i < result.getComponentCount(); i++)
        {
            DataComponent comp = result.getComponent(i);
            if (comp.isSetDefinition())
            {
                String def = comp.getDefinition();
                if (def.equals(SWEConstants.DEF_SAMPLING_TIME))
                {
                    samplingTime = comp.getData().getDoubleValue();
                }
            }
        }

        long samplingTimeMillis = (long)(samplingTime*1000.);
        TimeExtent phenTime = TimeExtent.instant(Instant.ofEpochMilli(samplingTimeMillis));

        // use same value for resultTime for now
        Instant resultTime = phenTime.begin();

        // observation property URI
        String obsPropDef = result.getDefinition();
        if (obsPropDef == null)
            obsPropDef = SWEConstants.NIL_UNKNOWN;
        
        // foi
        if (foiURI == null)
            foiURI = SWEConstants.NIL_UNKNOWN;            

        // create observation object        
        ObservationImpl obs = new ObservationImpl();
        obs.setType(getObsType(result));
        obs.setFeatureOfInterest(new FeatureRef<IGeoFeature>(foiURI));
        obs.setObservedProperty(new DefinitionRef(obsPropDef));
        obs.setProcedure(new ProcedureRef(procURI));
        obs.setPhenomenonTime(phenTime);
        obs.setResultTime(resultTime);
        obs.setResult(result);

        return obs;
    }
}
