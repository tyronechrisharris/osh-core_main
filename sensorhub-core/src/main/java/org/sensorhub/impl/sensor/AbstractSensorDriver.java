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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import net.opengis.gml.v32.Point;
import net.opengis.gml.v32.impl.GMLFactory;
import net.opengis.sensorml.v20.AbstractProcess;
import org.sensorhub.api.command.IStreamingControlInterface;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.IEventHandler;
import org.sensorhub.api.event.IEventListener;
import org.sensorhub.api.sensor.ISensorDriver;
import org.sensorhub.api.system.ISystemDriver;
import org.sensorhub.api.system.ISystemGroupDriver;
import org.sensorhub.api.system.SystemChangedEvent;
import org.sensorhub.api.utils.OshAsserts;
import org.sensorhub.impl.event.BasicEventHandler;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.om.SamplingPoint;
import org.vast.sensorML.PhysicalSystemImpl;
import org.vast.swe.SWEConstants;
import org.vast.util.Asserts;
import com.google.common.collect.ImmutableMap;


/**
 * <p>
 * Class providing default implementation of common sensor API methods.<br/>
 * This can be used as the base for most sensor driver implementations as it
 * generates defaults for the following:
 * <ul>
 * <li>A unique ID using a UUID (the same is used between restarts)</li>
 * <li>A short XML ID</li>
 * <li>A default SensorML description including IDs, temporal validity, I/Os
 * and position (location + orientation) if a static location and/or
 * orientation is provided</li>
 * <li>A feature of interest if the sensor configuration provides static
 * location</li>
 * </ul>
 * </p>
 *
 * @author Alex Robin
 * @since May 19, 2021
 */
public abstract class AbstractSensorDriver implements ISensorDriver
{
    public static final String DEFAULT_XMLID_PREFIX = "SENSOR_";
    protected static final String LOCATION_OUTPUT_ID = "SENSOR_LOCATION";
    protected static final String LOCATION_OUTPUT_NAME = "sensorLocation";

    private final Map<String, IStreamingDataInterface> obsOutputs = new LinkedHashMap<>();
    private final Map<String, IStreamingDataInterface> statusOutputs = new LinkedHashMap<>();
    private final Map<String, IStreamingControlInterface> controlInputs = new LinkedHashMap<>();

    protected final String uniqueID;
    protected final String shortID;
    protected final ISystemGroupDriver<? extends ISystemDriver> parentGroup;
    protected final IEventHandler eventHandler;
        
    protected volatile DefaultLocationOutput locationOutput;
    protected volatile AbstractProcess smlDescription = new PhysicalSystemImpl();
    protected volatile long lastUpdatedSensorDescription = Long.MIN_VALUE;

    protected volatile boolean enabled;
    protected volatile Map<String, IFeature> foiMap;

    
    protected AbstractSensorDriver(String uid, String shortID)
    {
        this(null, uid, shortID);
    }
    
    
    protected AbstractSensorDriver(ISystemGroupDriver<? extends ISystemDriver> parentGroup, String uid, String shortID)
    {
        this.uniqueID = OshAsserts.checkValidUID(uid);
        this.shortID = Asserts.checkNotNullOrEmpty(shortID, "shortID");
        this.parentGroup = parentGroup;
        this.eventHandler = new BasicEventHandler();
        this.foiMap = Collections.emptyMap();
    }


    /**
     * Adds a new observation or status output
     * @param dataInterface interface to add as sensor output
     * @param isStatus set to true when registering a status output
     */
    protected void addOutput(IStreamingDataInterface dataInterface, boolean isStatus)
    {
        Asserts.checkNotNull(dataInterface, IStreamingDataInterface.class);
        
        synchronized(obsOutputs)
        {
            if (isStatus)
                statusOutputs.put(dataInterface.getName(), dataInterface);
            else
                obsOutputs.put(dataInterface.getName(), dataInterface);
        }
    }


    /**
     * Sets the foi to be a sampling point at lat/lon/alt location (EPSG 4979).
     */
    protected void setSamplingPointFoi(double lat, double lon, double alt)
    {
        SamplingPoint sf = new SamplingPoint();
        sf.setId("FOI_" + getShortID());
        sf.setUniqueIdentifier(getUniqueIdentifier() + ":foi");
        sf.setName(getName());
        sf.setDescription("Sampling point for " + getName());
        sf.setHostedProcedureUID(getUniqueIdentifier());
        Point point = new GMLFactory(true).newPoint();
        point.setSrsName(SWEConstants.REF_FRAME_4979);
        point.setSrsDimension(3);
        point.setPos(new double[] {lat, lon, alt});
        sf.setShape(point);
        foiMap = ImmutableMap.of(sf.getUniqueIdentifier(), sf);
    }
    
    
    protected void addLocationOutput(double updatePeriod)
    {        
        synchronized(obsOutputs)
        {
            if (locationOutput == null)
            {
                // TODO deal with other CRS than 4979
                locationOutput = new DefaultLocationOutputLLA(this, getLocalReferenceFrame(), updatePeriod);
                addOutput(locationOutput, true);
            }
        }
    }


    /**
     * Removes all outputs previously added to this sensor
     */
    protected void removeAllOutputs()
    {
        synchronized(obsOutputs)
        {
            statusOutputs.clear();
            obsOutputs.clear();
        }
    }


    /**
     * Adds a new control input
     * @param controlInterface interface to add as sensor control input
     */
    protected void addControlInput(IStreamingControlInterface controlInterface)
    {
        Asserts.checkNotNull(controlInterface, IStreamingControlInterface.class);
        
        synchronized(controlInputs)
        {
            controlInputs.put(controlInterface.getName(), controlInterface);
        }
    }


    /**
     * Removes all control inputs previously added to this sensor
     */
    protected void removeAllControlInputs()
    {
        synchronized(controlInputs)
        {
            controlInputs.clear();
        }
    }
    
    
    @Override
    public String getName()
    {
        return "Sensor " + getShortID();
    }
    
    
    @Override
    public String getDescription()
    {
        return smlDescription != null ? smlDescription.getDescription() : null;
    }


    @Override
    public String getUniqueIdentifier()
    {
        return uniqueID;
    }
    
    
    public String getShortID()
    {
        return shortID;
    }


    @Override
    public ISystemGroupDriver<? extends ISystemDriver> getParentGroup()
    {
        return parentGroup;
    }


    @Override
    public String getParentGroupUID()
    {
        return parentGroup != null ? parentGroup.getUniqueIdentifier() : null;
    }


    @Override
    public AbstractProcess getCurrentDescription()
    {
        return smlDescription;
    }
    
    
    protected String getLocalReferenceFrame()
    {
        return "SENSOR_FRAME_" + getShortID().toUpperCase();
    }


    @Override
    public long getLatestDescriptionUpdate()
    {
        return lastUpdatedSensorDescription;
    }


    protected void notifyNewDescription(long updateTime)
    {
        // send event
        lastUpdatedSensorDescription = updateTime;
        eventHandler.publish(new SystemChangedEvent(updateTime, uniqueID));
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getOutputs()
    {
        Map<String, IStreamingDataInterface> allOutputs = new LinkedHashMap<>();
        allOutputs.putAll(obsOutputs);
        allOutputs.putAll(statusOutputs);
        return Collections.unmodifiableMap(allOutputs);
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getStatusOutputs()
    {
        return Collections.unmodifiableMap(statusOutputs);
    }


    @Override
    public Map<String, ? extends IStreamingDataInterface> getObservationOutputs()
    {
        return Collections.unmodifiableMap(obsOutputs);
    }


    @Override
    public Map<String, IStreamingControlInterface> getCommandInputs()
    {
        return Collections.unmodifiableMap(controlInputs);
    }


    @Override
    public Map<String, ? extends IFeature> getCurrentFeaturesOfInterest()
    {
        return Collections.unmodifiableMap(foiMap);
    }


    @Override
    public boolean isEnabled()
    {
        return enabled;
    }
    
    
    @Override
    public void registerListener(IEventListener listener)
    {
        eventHandler.registerListener(listener);
    }


    @Override
    public void unregisterListener(IEventListener listener)
    {
        eventHandler.unregisterListener(listener);
    }
}
