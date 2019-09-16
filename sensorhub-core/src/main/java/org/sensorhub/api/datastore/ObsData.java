/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.HashMap;
import java.util.Map;
import org.vast.util.Asserts;
import com.vividsolutions.jts.geom.Geometry;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * Immutable object representing observation data (result, foi ID, sampling
 * geometry, validity period, observation parameters) stored in an observation
 * store.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class ObsData
{
    private Map<String, Object> parameters = null;
    private Geometry phenomenonLocation = null;
    //private Range<Instant> validTime = null;
    private DataBlock result;
    
    
    /*
     * this class can only be instantiated using builder
     */
    ObsData()
    {        
    }
    
    
    /**
     * @return Observation parameters map
     */
    public Map<String, Object> getParameters()
    {
        return parameters;
    }


    /**
     * @return Area or volume (2D or 3D) where the observation was made.<br/>
     * If value is null, FoI geometry is used instead when provided. If neither geometry is provided,
     * observation will never be selected when filtering on geometry.<br/>
     * In a given data store, all geometries must be expressed in the same coordinate reference system.
     */    
    public Geometry getPhenomenonLocation()
    {
        return phenomenonLocation;
    }


    /**
     * @return Observation result data record
     */
    public DataBlock getResult()
    {
        return result;
    }


    public static ObsBuilder builder()
    {
        return new ObsBuilder();
    }
    
    
    public static class ObsBuilder
    {
        private ObsData instance = new ObsData();


        public ObsBuilder withParameter(String key, Object value)
        {
            if (instance.parameters == null)
                instance.parameters = new HashMap<>();
            instance.parameters.put(key, value);
            return this;
        }


        public ObsBuilder withPhenomenonLocation(Geometry phenomenonLocation)
        {
            instance.phenomenonLocation = phenomenonLocation;
            return this;
        }


        public ObsBuilder withResult(DataBlock result)
        {
            instance.result = result;
            return this;
        }
        
        
        public ObsData build()
        {
            Asserts.checkNotNull(instance.result, "result");
            return instance;
        }
    }
}
