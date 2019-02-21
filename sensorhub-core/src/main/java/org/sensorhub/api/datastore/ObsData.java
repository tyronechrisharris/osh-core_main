/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
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
 * geometry, observation parameters) stored in an observation store.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class ObsData
{
    private Map<String, Object> parameters = null;
    private Geometry phenomenonLocation = null;
    private DataBlock result;
    private short version = 0; // version of result structure
    
    
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
    
    
    /**
     * @return Version of observation result structure
     */
    public short getVersion()
    {
        return version;
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


        public ObsBuilder withVersion(short version)
        {
            instance.version = version;
            return this;
        }
        
        
        public ObsData build()
        {
            Asserts.checkNotNull(instance.result, "result");
            return instance;
        }
    }
}
