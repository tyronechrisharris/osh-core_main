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
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.event.Event;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.obs.DataStreamAddedEvent;
import org.sensorhub.api.obs.DataStreamDisabledEvent;
import org.sensorhub.api.obs.DataStreamEnabledEvent;
import org.sensorhub.api.obs.DataStreamEvent;
import org.sensorhub.api.obs.DataStreamRemovedEvent;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureDisabledEvent;
import org.sensorhub.api.procedure.ProcedureEnabledEvent;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.api.sensor.SensorConfig;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.sensor.FakeSensor;
import org.sensorhub.impl.sensor.FakeSensorData;
import org.sensorhub.impl.sensor.FakeSensorData2;
import org.sensorhub.impl.sensor.FakeSensorNetOnlyFois;
import org.sensorhub.impl.sensor.FakeSensorNetWithMembers;
import org.sensorhub.test.AsyncTests;
import com.google.common.collect.Sets;


public class TestProcedureRegistry
{
    static final String NAME_OUTPUT1 = "weather";
    static final String NAME_OUTPUT2 = "image";
    static final double SAMPLING_PERIOD = 0.1;
    static final int NUM_GEN_SAMPLES = 5;
    static final String NO_FOI = "none";
    
    ISensorHub hub;
    IProcedureRegistry registry;
    IProcedureObsDatabase stateDb;
    IProcedureObsDatabase otherDb;
    
    
    @Before
    public void init()
    {
        hub = new SensorHub();
        hub.start();
        registry = hub.getProcedureRegistry();
        stateDb = hub.getProcedureRegistry().getProcedureStateDatabase();
    }
    
    
    @Test
    public void testRegisterSimpleSensor() throws Exception
    {
        // configure and init sensor
        FakeSensor sensor = new FakeSensor();
        sensor.setConfiguration(new SensorConfig());
        sensor.init();
        sensor.setDataInterfaces(
            new FakeSensorData(sensor, NAME_OUTPUT1, 0.1, NUM_GEN_SAMPLES),
            new FakeSensorData2(sensor, NAME_OUTPUT2, 0.05, NUM_GEN_SAMPLES));
                
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
            assertEquals(sensor.getCurrentDescription().getName() + " - " + sensor.getOutputs().get(NAME_OUTPUT1).getRecordDescription().getLabel(), ds1.getName());
            var ds2 = stateDb.getDataStreamStore().getLatestVersion(sensor.getUniqueIdentifier(), NAME_OUTPUT2);
            assertEquals(NAME_OUTPUT2, ds2.getOutputName());
            assertEquals(sensor.getCurrentDescription().getName() + " - " + sensor.getOutputs().get(NAME_OUTPUT2).getRecordDescription().getLabel(), ds2.getName());   
            
        })
        .thenCompose(nil -> {
            // check data is forwarded to event bus
            var topic = EventUtils.getDataStreamDataTopicID(sensor.getOutputs().get(NAME_OUTPUT1));
            System.out.println("Subscribing to channel " + topic);
            hub.getEventBus().newSubscription(DataEvent.class)
                .withEventType(DataEvent.class)
                .withTopicID(topic)
                .consume(e -> {
                    System.out.println("Record received from " + e.getSourceID() + ", ts=" +
                        Instant.ofEpochMilli(e.getTimeStamp()));
                    sampleCounter.incrementAndGet();
                });
            
            return sensor.startSendingData();
            //return ((IFakeSensorOutput)sensor.getOutputs().get(NAME_OUTPUT1)).start(false);
        })
        .thenRun(() -> {            
            // check latest record is in DB
            assertEquals(2, stateDb.getObservationStore().size());
        })
        .join();
        
        // check we received all records from event bus
        AsyncTests.waitForCondition(() -> sampleCounter.get() >= NUM_GEN_SAMPLES, 1000L);
        assertEquals(NUM_GEN_SAMPLES, sampleCounter.get());
    }
    
    
    @Test
    public void testAutoRegisterSimpleSensor() throws Exception
    {
        FakeSensor sensor = new FakeSensor();
        sensor.setParentHub(hub);
        sensor.setConfiguration(new SensorConfig());
        sensor.init();
        sensor.setDataInterfaces(new FakeSensorData(sensor, NAME_OUTPUT1, 1.0, 10));
        sensor.start();
        
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
        sensorNet.init();
        sensorNet.addMembers(numMembers, p -> {
            p.addOutputs(
                new FakeSensorData(p, NAME_OUTPUT1, SAMPLING_PERIOD, NUM_GEN_SAMPLES),
                new FakeSensorData2(p, NAME_OUTPUT2, 0.1, NUM_GEN_SAMPLES*2));
        });
        
        assertEquals(0, stateDb.getProcedureStore().size());
        assertEquals(0, stateDb.getFoiStore().size());
        
        AtomicInteger sampleCounter = new AtomicInteger();
        Map<String, Integer> sampleCountsPerMember = new ConcurrentHashMap<>();
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
                var sensorName = sensor.getCurrentDescription().getName();
                for (var output: sensor.getOutputs().values())
                {
                    var ds = stateDb.getDataStreamStore().getLatestVersion(sensor.getUniqueIdentifier(), output.getName());
                    assertEquals(output.getName(), ds.getOutputName());
                    assertEquals(sensorName + " - " + output.getRecordDescription().getLabel(), ds.getName());
                }
            }           
        })
        .thenCompose(nil -> {
            // check data is forwarded to event bus
            var subBuilder = hub.getEventBus().newSubscription(DataEvent.class)
                .withEventType(DataEvent.class);
            
            for (var sensor: sensorNet.getMembers().values())
            {
                var topic = EventUtils.getDataStreamDataTopicID(sensor.getOutputs().get(NAME_OUTPUT1));
                System.out.println("Subscribe to channel " + topic);
                subBuilder.withTopicID(topic);                     
            }
            
            subBuilder.consume(e -> {
                System.out.println("Record received from " + e.getSourceID() + ", ts=" +
                    Instant.ofEpochMilli(e.getTimeStamp()));
                sampleCounter.incrementAndGet();
                sampleCountsPerMember.compute(e.getProcedureUID(), (k, v) -> {
                    if (v == null)
                        return 1;
                    else
                        return v+1;
                });
            });
            
            return sensorNet.startSendingData();
        })
        .thenRun(() -> {            
            // check latest records are in DB
            assertEquals(numMembers*2, stateDb.getObservationStore().size());
        })
        .join();
        
        // check we received all records from event bus
        AsyncTests.waitForCondition(() -> sampleCounter.get() >= numMembers*NUM_GEN_SAMPLES, 1000L);
        for (String uid: sensorNet.getMembers().keySet())
            assertEquals(NUM_GEN_SAMPLES, (int)sampleCountsPerMember.get(uid));
    }
        
        
    @Test
    public void testRegisterSensorArray() throws Exception
    {
        int numFois = 5;
        int numObs = 24;
        var obsFoiMap = new TreeMap<Integer, Integer>();
        obsFoiMap.put(3, 1);
        obsFoiMap.put(5, 3);
        obsFoiMap.put(8, 2);
        obsFoiMap.put(12, 4);
        obsFoiMap.put(15, 2);
        obsFoiMap.put(17, 3);
        obsFoiMap.put(19, 5);
        FakeSensorNetOnlyFois sensorNet = new FakeSensorNetOnlyFois();
        sensorNet.setConfiguration(new SensorConfig());
        sensorNet.init();
        sensorNet.setDataInterfaces(new FakeSensorData2(sensorNet, NAME_OUTPUT2, 0.05, numObs, obsFoiMap));
        sensorNet.addFois(numFois);
        checkSensorArray(sensorNet, numFois, numObs, obsFoiMap);
    }
    
    
    @Test
    public void testRegisterSensorArrayWithDynamicFois() throws Exception
    {
        int numFois = 5;
        int numObs = 100;
        var obsFoiMap = new TreeMap<Integer, Integer>();
        obsFoiMap.put(3, 1);
        obsFoiMap.put(4, 2);
        obsFoiMap.put(8, 1);
        obsFoiMap.put(10, 15);
        obsFoiMap.put(12, 4);
        obsFoiMap.put(15, 2);
        obsFoiMap.put(17, 3);
        obsFoiMap.put(19, 5);
        obsFoiMap.put(21, 55);
        FakeSensorNetOnlyFois sensorNet = new FakeSensorNetOnlyFois();
        sensorNet.setParentHub(hub);
        sensorNet.setConfiguration(new SensorConfig());
        sensorNet.init();
        sensorNet.setDataInterfaces(new FakeSensorData2(sensorNet, NAME_OUTPUT2, 0.05, numObs, obsFoiMap));
        sensorNet.addFois(numFois);
        checkSensorArray(sensorNet, numFois, numObs, obsFoiMap);
    }
    
    
    protected void checkSensorArray(FakeSensorNetOnlyFois sensorNet, int numInitFois, int numObs, TreeMap<Integer, Integer> obsFoiMap) throws Exception
    {
        assertEquals(0, stateDb.getProcedureStore().size());
        assertEquals(0, stateDb.getFoiStore().size());
        
        AtomicInteger sampleCounter = new AtomicInteger();
        Map<String, Integer> sampleCountsPerFoi = new TreeMap<>();
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
            assertEquals(numInitFois, stateDb.getFoiStore().size());
            
            // check no members are in DB
            var numRegisteredMembers = stateDb.getProcedureStore().countMatchingEntries(new ProcedureFilter.Builder()
                .withParents(sensorNet.getUniqueIdentifier())
                .build());
            assertEquals(0, numRegisteredMembers);
            
            // check datastreams are in DB
            System.out.println(stateDb.getDataStreamStore().size() + " datastream(s) registered");
            for (var output: sensorNet.getOutputs().values())
            {
                var sensorName = sensorNet.getCurrentDescription().getName();
                var ds = stateDb.getDataStreamStore().getLatestVersion(sensorNet.getUniqueIdentifier(), output.getName());
                assertEquals(output.getName(), ds.getOutputName());
                assertEquals(sensorName + " - " + output.getRecordDescription().getLabel(), ds.getName());
            }           
        })
        .thenCompose(nil -> {
            // check data is forwarded to event bus
            var topic = EventUtils.getDataStreamDataTopicID(sensorNet.getOutputs().get(NAME_OUTPUT2));
            System.out.println("Subscribe to channel " + topic);
            hub.getEventBus().newSubscription(DataEvent.class)
                .withEventType(DataEvent.class)
                .withTopicID(topic)
                .consume(e -> {
                    var foiStr = e.getFoiUID() != null ? e.getFoiUID() : NO_FOI;
                    System.out.println("Record received from " + e.getSourceID() +
                        ", ts=" + Instant.ofEpochMilli(e.getTimeStamp()) +
                        ", foi=" + foiStr);
                    sampleCounter.incrementAndGet();
                    sampleCountsPerFoi.compute(foiStr, (k, v) -> {
                        if (v == null)
                            return 1;
                        else
                            return v+1;
                    });
                });
            
            return sensorNet.startSendingData();
        })
        .thenRun(() -> {
            // check latest records are in DB (one per foi)
            var observedFois = Sets.newHashSet(obsFoiMap.values());
            assertEquals(observedFois.size()+1, stateDb.getObservationStore().size());
        })
        .join();
        
        // check we received all records from event bus
        AsyncTests.waitForCondition(() -> sampleCounter.get() >= numObs, 1000L);
        for (var sampleCountEntry: sampleCountsPerFoi.entrySet())
        {
            Integer expectedCount;
            var foiUID = sampleCountEntry.getKey();
            if (NO_FOI.equals(foiUID))
                expectedCount = obsFoiMap.ceilingKey(0)-1;
            else
            {
                expectedCount = obsFoiMap.entrySet().stream()
                    .filter(e -> {
                        var uid = sensorNet.getFoiUID(e.getValue());
                        return uid.equals(sampleCountEntry.getKey());  
                    })
                    .map(e -> {
                        var nextKey = obsFoiMap.higherKey(e.getKey());
                        return (nextKey != null ? nextKey : numObs+1)- e.getKey();
                    })
                    .reduce(0, Integer::sum);
            }
            
            assertEquals(expectedCount, sampleCountEntry.getValue());
        }
    }
    
    
    @Test
    public void testRegisterAndCheckPublishedEvents() throws Exception
    {
        // configure and init sensor
        FakeSensor sensor = new FakeSensor();
        sensor.setConfiguration(new SensorConfig());
        sensor.init();
        sensor.setDataInterfaces(
            new FakeSensorData(sensor, NAME_OUTPUT1, 0.1, NUM_GEN_SAMPLES),
            new FakeSensorData2(sensor, NAME_OUTPUT2, 0.05, NUM_GEN_SAMPLES));        
        
        // subscribe to events
        var receivedEvents = new ArrayList<Event>();
        hub.getEventBus().newSubscription(ProcedureEvent.class)
            .withTopicID(EventUtils.getProcedureRegistryTopicID())
            .withTopicID(EventUtils.getProcedureStatusTopicID(sensor))
            .consume(e -> {
                System.out.println("Received " + e.getClass().getSimpleName() +
                    " from " + e.getProcedureUID() +
                    ((e instanceof DataStreamEvent) ? ", output=" + ((DataStreamEvent)e).getOutputName() : "") );
                receivedEvents.add(e);
            });        
        
        // register
        registry.register(sensor).join();
        Thread.sleep(100);
                    
        // check that we received all "added" events
        assertEquals(sensor.getOutputs().size(), receivedEvents.stream()
            .filter(e -> e instanceof DataStreamAddedEvent)
            .count());
        
        assertEquals(sensor.getOutputs().size(), receivedEvents.stream()
            .filter(e -> e instanceof DataStreamEnabledEvent)
            .count());
        
        assertEquals(1, receivedEvents.stream()
            .filter(e -> e instanceof ProcedureAddedEvent)
            .count());
        
        assertEquals(1, receivedEvents.stream()
            .filter(e -> e instanceof ProcedureEnabledEvent)
            .count());
        
        assertEquals(sensor.getOutputs().size()*2+2, receivedEvents.size());
        
        
        // register again and check that we receive proper events
        receivedEvents.clear();
        registry.register(sensor).join();
        Thread.sleep(100);
        
        assertEquals(sensor.getOutputs().size(), receivedEvents.stream()
            .filter(e -> e instanceof DataStreamEnabledEvent)
            .count());
        
        assertEquals(1, receivedEvents.stream()
            .filter(e -> e instanceof ProcedureEnabledEvent)
            .count());
        
        assertEquals(0, receivedEvents.stream()
            .filter(e -> e instanceof DataStreamAddedEvent)
            .count());
        
        assertEquals(0, receivedEvents.stream()
            .filter(e -> e instanceof ProcedureAddedEvent)
            .count());
        
        assertEquals(sensor.getOutputs().size()+1, receivedEvents.size());
    }
    
    
    @Test
    public void testUnregisterAndCheckPublishedEvents() throws Exception
    {
        // configure and init sensor
        FakeSensor sensor = new FakeSensor();
        sensor.setConfiguration(new SensorConfig());
        sensor.init();
        sensor.setDataInterfaces(
            new FakeSensorData(sensor, NAME_OUTPUT1, 0.1, NUM_GEN_SAMPLES),
            new FakeSensorData(sensor, NAME_OUTPUT1 + "_bis", 0.1, NUM_GEN_SAMPLES),
            new FakeSensorData2(sensor, NAME_OUTPUT2, 0.05, NUM_GEN_SAMPLES));        
        
        // subscribe to events
        var receivedEvents = new ArrayList<Event>();
        hub.getEventBus().newSubscription(ProcedureEvent.class)
            .withTopicID(EventUtils.getProcedureRegistryTopicID())
            .withTopicID(EventUtils.getProcedureStatusTopicID(sensor))
            .consume(e -> {
                System.out.println("Received " + e.getClass().getSimpleName() +
                    " from " + e.getProcedureUID() +
                    ((e instanceof DataStreamEvent) ? ", output=" + ((DataStreamEvent)e).getOutputName() : "") );
                receivedEvents.add(e);
            });        
        
        // register
        registry.register(sensor).join();
        Thread.sleep(100);
        
        // unregister and check that we receive proper events
        receivedEvents.clear();
        registry.unregister(sensor).join();
        Thread.sleep(100);
        
        assertEquals(sensor.getOutputs().size(), receivedEvents.stream()
            .filter(e -> e instanceof DataStreamDisabledEvent)
            .count());
        
        assertEquals(1, receivedEvents.stream()
            .filter(e -> e instanceof ProcedureDisabledEvent)
            .count());
        
        assertEquals(0, receivedEvents.stream()
            .filter(e -> e instanceof DataStreamRemovedEvent)
            .count());
        
        assertEquals(0, receivedEvents.stream()
            .filter(e -> e instanceof ProcedureRemovedEvent)
            .count());
        
        assertEquals(sensor.getOutputs().size()+1, receivedEvents.size());        
    }

}
