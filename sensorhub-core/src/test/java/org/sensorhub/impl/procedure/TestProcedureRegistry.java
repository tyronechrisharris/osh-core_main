/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.sensor.SensorConfig;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.sensor.FakeSensor;
import org.sensorhub.impl.sensor.FakeSensorData;
import org.sensorhub.impl.sensor.FakeSensorData2;
import org.sensorhub.impl.sensor.FakeSensorNetOnlyFois;
import org.sensorhub.impl.sensor.FakeSensorNetWithMembers;


public class TestProcedureRegistry
{
    static final String NAME_OUTPUT1 = "weather";
    static final String NAME_OUTPUT2 = "image";
    static final double SAMPLING_PERIOD = 0.1;
    static final int NUM_GEN_SAMPLES = 5;
    
    ISensorHub hub;
    IProcedureRegistry registry;
    IProcedureObsDatabase stateDb;
    IProcedureObsDatabase otherDb;
    
    
    @Before
    public void init()
    {
        hub = new SensorHub();
        registry = hub.getProcedureRegistry();
        stateDb = hub.getProcedureRegistry().getProcedureStateDatabase();
    }
    
    
    @Test
    public void testRegisterSimpleSensor() throws Exception
    {
        // configure and init sensor
        FakeSensor sensor = new FakeSensor();
        sensor.setConfiguration(new SensorConfig());
        sensor.setDataInterfaces(
            new FakeSensorData(sensor, NAME_OUTPUT1, 0.1, NUM_GEN_SAMPLES),
            new FakeSensorData2(sensor, NAME_OUTPUT2, 0.05, NUM_GEN_SAMPLES));
        sensor.requestInit(true);
        
        assertEquals(0, stateDb.getProcedureStore().size());
        
        AtomicInteger sampleCounter = new AtomicInteger();
        registry.register(sensor).thenRun(() -> {
            
            // check procedure is in DB
            assertEquals(1, stateDb.getProcedureStore().size());
            var proc = stateDb.getProcedureStore().getCurrentVersion(sensor.getUniqueIdentifier());
            assertNotNull("Sensor not registered", proc);
            assertEquals(sensor.getUniqueIdentifier(), proc.getUniqueIdentifier());
            
            // check datastreams are in DB
            assertEquals(2, stateDb.getDataStreamStore().size());
            var ds1 = stateDb.getDataStreamStore().getLatestVersion(sensor.getUniqueIdentifier(), NAME_OUTPUT1);
            assertEquals(NAME_OUTPUT1, ds1.getOutputName());
            assertEquals(sensor.getOutputs().get(NAME_OUTPUT1).getRecordDescription().getLabel(), ds1.getName());
            var ds2 = stateDb.getDataStreamStore().getLatestVersion(sensor.getUniqueIdentifier(), NAME_OUTPUT2);
            assertEquals(NAME_OUTPUT2, ds2.getOutputName());
            assertEquals(sensor.getOutputs().get(NAME_OUTPUT2).getRecordDescription().getLabel(), ds2.getName());   
            
        })
        .thenCompose(v -> {
            // check data is forwarded to event bus
            var eventSrcInfo = sensor.getOutputs().get(NAME_OUTPUT1).getEventSourceInfo();
            System.out.println("Subscribe to channel " + eventSrcInfo);
            hub.getEventBus().newSubscription(DataEvent.class)
                .withEventType(DataEvent.class)
                .withSourceInfo(eventSrcInfo)
                .consume(e -> {
                    System.out.println("Record received from " + e.getSourceID() + ", ts=" +
                        Instant.ofEpochMilli(e.getTimeStamp()));
                    sampleCounter.incrementAndGet();
                });                 
            
            return sensor.startSendingData(false);
            //return ((IFakeSensorOutput)sensor.getOutputs().get(NAME_OUTPUT1)).start(false);
        })
        .thenRun(() -> {            
            // check latest record is in DB
            assertEquals(2, stateDb.getObservationStore().size());
        })
        .join();
        
        // check we received all records from event bus
        assertEquals(NUM_GEN_SAMPLES, sampleCounter.get());
    }
    
    
    @Test
    public void testAutoRegisterSimpleSensor() throws Exception
    {
        FakeSensor sensor = new FakeSensor();
        sensor.setParentHub(hub);
        sensor.setConfiguration(new SensorConfig());
        sensor.setDataInterfaces(new FakeSensorData(sensor, NAME_OUTPUT1, 1.0, 10));
        sensor.requestInit(true);
        
        // check procedure is in DB
        assertEquals(1, stateDb.getProcedureStore().size());
        var proc = stateDb.getProcedureStore().getCurrentVersion(sensor.getUniqueIdentifier());
        assertEquals(sensor.getUniqueIdentifier(), proc.getUniqueIdentifier());
    }
    
    
    @Test
    public void testRegisterSensorGroup() throws Exception
    {
        int numMembers = 10;
        FakeSensorNetWithMembers sensorNet = new FakeSensorNetWithMembers();
        sensorNet.setConfiguration(new SensorConfig());
        sensorNet.addMembers(numMembers, p -> {
            p.addOutputs(
                new FakeSensorData(p, NAME_OUTPUT1, SAMPLING_PERIOD, NUM_GEN_SAMPLES),
                new FakeSensorData2(p, NAME_OUTPUT2, 0.1, NUM_GEN_SAMPLES*2));
        });
        sensorNet.requestInit(true);
        
        assertEquals(0, stateDb.getProcedureStore().size());
        assertEquals(0, stateDb.getFoiStore().size());
        
        Map<String, Integer> sampleCounters = new HashMap<>();
        registry.register(sensorNet).thenRun(() -> {
            
            // check parent procedure is in DB
            assertEquals(numMembers+1, stateDb.getProcedureStore().size());
            var proc = stateDb.getProcedureStore().getCurrentVersion(sensorNet.getUniqueIdentifier());
            assertNotNull("SensorNet not registered", proc);
            assertEquals(sensorNet.getUniqueIdentifier(), proc.getUniqueIdentifier());
            
            // check all fois are in DB
            System.out.println(stateDb.getFoiStore().size() + " FOIs registered");
            for (var foiUID: sensorNet.getCurrentFeaturesOfInterest().keySet())
                assertTrue("Missing FOI in DB: " + foiUID, stateDb.getFoiStore().contains(foiUID));
            assertEquals(numMembers, stateDb.getFoiStore().size());
            
            // check all members are in DB
            for (var memberUID: sensorNet.getMembers().keySet())
                assertTrue("Missing child procedure in DB: " + memberUID, stateDb.getProcedureStore().contains(memberUID));
            var numRegisteredMembers = stateDb.getProcedureStore().countMatchingEntries(new ProcedureFilter.Builder()
                .withParents(sensorNet.getUniqueIdentifier())
                .build());
            System.out.println(numRegisteredMembers + " child procedures registered");
            assertEquals(numMembers, numRegisteredMembers);
            
            // check datastreams are in DB
            System.out.println(stateDb.getDataStreamStore().size() + " datastream(s) registered");
            for (var sensor: sensorNet.getMembers().values())
            {
                for (var output: sensor.getOutputs().values())
                {
                    var ds = stateDb.getDataStreamStore().getLatestVersion(sensor.getUniqueIdentifier(), output.getName());
                    assertEquals(output.getName(), ds.getOutputName());
                    assertEquals(output.getRecordDescription().getLabel(), ds.getName());
                }
            }           
        })
        .thenCompose(nil -> {
            // check data is forwarded to event bus
            var subBuilder = hub.getEventBus().newSubscription(DataEvent.class)
                .withEventType(DataEvent.class);
            
            for (var sensor: sensorNet.getMembers().values())
            {
                var eventSrcInfo = sensor.getOutputs().get(NAME_OUTPUT1).getEventSourceInfo();
                System.out.println("Subscribe to channel " + eventSrcInfo);
                subBuilder.withSourceInfo(eventSrcInfo);                     
            }
            
            subBuilder.consume(e -> {
                System.out.println("Record received from " + e.getSourceID() + ", ts=" +
                    Instant.ofEpochMilli(e.getTimeStamp()));
                sampleCounters.compute(e.getProcedureUID(), (k, v) -> {
                    if (v == null)
                        return 1;
                    else
                        return v+1;
                });
            });
            
            return sensorNet.startSendingData(false);
        })
        .thenRun(() -> {            
            // check latest records are in DB
            assertEquals(numMembers*2, stateDb.getObservationStore().size());            
        })
        .join();
        
        // check we received all records from event bus
        for (String uid: sensorNet.getMembers().keySet())
            assertEquals(NUM_GEN_SAMPLES, (int)sampleCounters.get(uid));
    }
        
        
    @Test
    public void testRegisterSensorArray() throws Exception
    {
        int numFois = 50;
        FakeSensorNetOnlyFois sensorNet = new FakeSensorNetOnlyFois();
        sensorNet.setConfiguration(new SensorConfig());
        sensorNet.setDataInterfaces(new FakeSensorData(sensorNet, NAME_OUTPUT1, 1.0, 10));
        sensorNet.addFois(numFois);
        sensorNet.requestInit(true);
        
        assertEquals(0, stateDb.getProcedureStore().size());
        assertEquals(0, stateDb.getFoiStore().size());
        
        Map<String, Integer> sampleCounters = new HashMap<>();
        registry.register(sensorNet).thenRun(() -> {
            
            // check parent procedure is in DB
            assertEquals(1, stateDb.getProcedureStore().size());
            var proc = stateDb.getProcedureStore().getCurrentVersion(sensorNet.getUniqueIdentifier());
            assertNotNull("SensorNet not registered", proc);
            assertEquals(sensorNet.getUniqueIdentifier(), proc.getUniqueIdentifier());
            
            // check all fois are in DB
            System.out.println(stateDb.getFoiStore().size() + " FOIs registered");
            for (var foiUID: sensorNet.getCurrentFeaturesOfInterest().keySet())
                assertTrue("Missing FOI in DB: " + foiUID, stateDb.getFoiStore().contains(foiUID));
            assertEquals(numFois, stateDb.getFoiStore().size());
            
            // check no members are in DB
            var numRegisteredMembers = stateDb.getProcedureStore().countMatchingEntries(new ProcedureFilter.Builder()
                .withParents(sensorNet.getUniqueIdentifier())
                .build());
            assertEquals(0, numRegisteredMembers);
            
            // check datastreams are in DB
            System.out.println(stateDb.getDataStreamStore().size() + " datastream(s) registered");
            for (var output: sensorNet.getOutputs().values())
            {
                var ds = stateDb.getDataStreamStore().getLatestVersion(sensorNet.getUniqueIdentifier(), output.getName());
                assertEquals(output.getName(), ds.getOutputName());
                assertEquals(output.getRecordDescription().getLabel(), ds.getName());
            }           
        })
        .thenCompose(nil -> {
            // check data is forwarded to event bus
            var eventSrcInfo = sensorNet.getOutputs().get(NAME_OUTPUT1).getEventSourceInfo();
            System.out.println("Subscribe to channel " + eventSrcInfo);
            hub.getEventBus().newSubscription(DataEvent.class)
                .withEventType(DataEvent.class)
                .withSourceInfo(eventSrcInfo)                     
                .consume(e -> {
                    System.out.println("Record received from " + e.getSourceID() + ", ts=" +
                        Instant.ofEpochMilli(e.getTimeStamp()));
                    sampleCounters.compute(e.getProcedureUID(), (k, v) -> {
                        if (v == null)
                            return 1;
                        else
                            return v+1;
                    });
                });
            
            return sensorNet.startSendingData(false);
        })
        .thenRun(() -> {            
            // check latest records are in DB
            assertEquals(numFois, stateDb.getObservationStore().size());            
        })
        .join();
        
        /*// check we received all records from event bus
        for (String uid: sensorNet.getMembers().keySet())
            assertEquals(NUM_GEN_SAMPLES, (int)sampleCounters.get(uid));*/
    }

}
