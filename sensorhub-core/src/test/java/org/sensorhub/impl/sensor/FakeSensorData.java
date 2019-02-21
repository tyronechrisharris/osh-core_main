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

import java.util.Timer;
import java.util.TimerTask;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.Quantity;
import net.opengis.swe.v20.Time;
import org.sensorhub.api.common.IEventListener;
import org.sensorhub.api.common.IEventPublisher;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.sensor.ISensorModule;
import org.sensorhub.api.sensor.SensorDataEvent;
import org.sensorhub.impl.sensor.AbstractSensorOutput;
import org.vast.data.DataBlockDouble;
import org.vast.data.DataRecordImpl;
import org.vast.data.QuantityImpl;
import org.vast.data.TextEncodingImpl;
import org.vast.data.TimeImpl;
import org.vast.swe.SWEConstants;
import org.vast.util.DateTimeFormat;


/**
 * <p>
 * Fake sensor output implementation for testing sensor data API
 * </p>
 *
 * @author Alex Robin
 * @since Sep 20, 2013
 */
public class FakeSensorData extends AbstractSensorOutput<IDataProducer> implements IFakeSensorOutput
{
    public static final String URI_OUTPUT1 = "urn:blabla:weatherData";
    public static final String URI_OUTPUT1_FIELD1 = "urn:blabla:temperature";
    public static final String URI_OUTPUT1_FIELD2 = "urn:blabla:windSpeed";
    public static final String URI_OUTPUT1_FIELD3 = "urn:blabla:pressure";
    
    String name;
    DataComponent outputStruct;
    DataEncoding outputEncoding;
    DataBlock latestRecord;
    int maxSampleCount;
    int sampleCount;
    double samplingPeriod; // seconds
    Timer timer;
    TimerTask sendTask;
    boolean started;
    boolean hasListeners;
    
    
    public FakeSensorData(FakeSensor sensor, String name)
    {
        this(sensor, name, 1.0, 5);
    }
    
    
    public FakeSensorData(ISensorModule<?> sensor, final String name, final double samplingPeriod, final int maxSampleCount)
    {
        super(name, sensor);
        this.name = name;
        this.samplingPeriod = samplingPeriod;
        this.maxSampleCount = maxSampleCount;
        init();
    }
    
    
    public FakeSensorData(IDataProducer sensor, final String name, final double samplingPeriod, final int maxSampleCount, IEventPublisher eventHandler)
    {
        super(name, sensor, eventHandler);
        this.name = name;
        this.samplingPeriod = samplingPeriod;
        this.maxSampleCount = maxSampleCount;
        init();
    }
    
    
    public void init()
    {
        outputStruct = new DataRecordImpl(3);
        outputStruct.setName(this.name);
        outputStruct.setDefinition(URI_OUTPUT1);
        
        Time time = new TimeImpl();
        time.setDefinition(SWEConstants.DEF_SAMPLING_TIME);
        time.getUom().setHref(Time.ISO_TIME_UNIT);
        outputStruct.addComponent("time", time);
        
        Quantity temp = new QuantityImpl();
        temp.setDefinition(URI_OUTPUT1_FIELD1);
        temp.getUom().setCode("Cel");
        outputStruct.addComponent("temp", temp);
        
        Quantity wind = new QuantityImpl();
        wind.setDefinition(URI_OUTPUT1_FIELD2);
        wind.getUom().setCode("m/s");
        outputStruct.addComponent("windSpeed", wind);
        
        Quantity press = new QuantityImpl();
        press.setDefinition(URI_OUTPUT1_FIELD3);
        press.getUom().setCode("hPa");
        outputStruct.addComponent("press", press);
        
        outputEncoding = new TextEncodingImpl(",", "\n");
    }


    public int getMaxSampleCount()
    {
        return maxSampleCount;
    }


    @Override
    public void start(boolean waitForListeners)
    {
        sendTask = new TimerTask()
        {
            @Override
            public void run()
            {
                // safety to make sure we don't output more samples than requested
                // cancel does not seem to be taken into account early enough with high rates
                if (sampleCount >= maxSampleCount)
                    return;
                
                // miss random samples 20% of the time
                if (Math.random() > 0.8)
                    return;
                
                double samplingTime = System.currentTimeMillis() / 1000.;
                DataBlock data = new DataBlockDouble(4);
                data.setDoubleValue(0, samplingTime);
                data.setDoubleValue(1, 1.0 + ((int)(Math.random()*100))/1000.);
                data.setDoubleValue(2, 2.0 + ((int)(Math.random()*100))/1000.);
                data.setDoubleValue(3, 3.0 + ((int)(Math.random()*100))/1000.);
                           
                sampleCount++;
                String isoTime = new DateTimeFormat().formatIso(samplingTime, 0);
                System.out.println("Weather record #" + sampleCount + " generated @ " + isoTime);
                if (sampleCount >= maxSampleCount)
                    cancel();
                
                latestRecord = data;
                latestRecordTime = System.currentTimeMillis();
                eventHandler.publishEvent(new SensorDataEvent(latestRecordTime, FakeSensorData.this, data));                        
            }                
        };
        
        // we start sending only if we have listeners
        if (hasListeners || !waitForListeners)
            startSending();
        started = true;
    }
    
    
    protected void startSending()
    {
        if (timer == null)
        {
            timer = new Timer(name, true);
            timer.scheduleAtFixedRate(sendTask, 0, (long)(samplingPeriod * 1000));
        }
    }


    @Override
    public DataBlock getLatestRecord()
    {
        return latestRecord;
    }
    
    
    @Override
    public void stop()
    {
        if (timer != null)
        {
            timer.cancel();
            timer.purge();
            timer = null;
        }
    }
    
    
    @Override
    public boolean isEnabled()
    {
        if (sampleCount >= maxSampleCount)
            return false;
        else
            return true;
    }


    @Override
    public double getAverageSamplingPeriod()
    {
        return samplingPeriod;
    }


    @Override
    public DataComponent getRecordDescription()
    {
        return outputStruct;
    }


    @Override
    public DataEncoding getRecommendedEncoding()
    {
        return outputEncoding;
    }


    @Override
    public void registerListener(IEventListener listener)
    {
        super.registerListener(listener);
        
        // we start sending only if start has been called
        if (started)
            startSending();
        
        hasListeners = true;
    }

}
