/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.sensorhub.api.datastore.DataStreamFilter;
import org.sensorhub.api.datastore.FeatureFilter;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IHistoricalObsDatabase;
import org.sensorhub.api.datastore.IDatabaseRegistry;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.procedure.InMemoryProcedureStateDatabase;
import org.vast.sensorML.SimpleProcessImpl;
import com.google.common.collect.Sets;
import net.opengis.sensorml.v20.AbstractProcess;


public class TestFederatedDatabaseRegistry
{
    IDatabaseRegistry registry;
    IHistoricalObsDatabase mainObsDatabase;
    
    
    @Rule
    public ExpectedException errors = ExpectedException.none();
    
    
    @Before
    public void setup()
    {
        SensorHub hub = new SensorHub();
        this.registry = hub.getDatabaseRegistry();
        this.mainObsDatabase = registry.getFederatedObsDatabase();
    }
    
    
    @Test
    public void testRegister()
    {
        registry.register(Sets.newHashSet("sensor1", "sensor2"), new InMemoryProcedureStateDatabase());
        registry.register(Sets.newHashSet("sensor3", "sensor4"), new InMemoryProcedureStateDatabase((byte)1));
    }
    
    
    @Test
    public void testRegisterDuplicateProcUIDs()
    {
        registry.register(Sets.newHashSet("sensor1", "sensor2"), new InMemoryProcedureStateDatabase((byte)1));
        errors.expect(IllegalStateException.class);
        registry.register(Sets.newHashSet("sensor2", "sensor4"), new InMemoryProcedureStateDatabase((byte)2));
    }
    
    
    @Test
    public void testEmptyDatabases()
    {
        registry.register(Sets.newHashSet("sensor1", "sensor2"), new InMemoryProcedureStateDatabase());
        registry.register(Sets.newHashSet("sensor3", "sensor4"), new InMemoryProcedureStateDatabase((byte)1));
        
        assertEquals(0, mainObsDatabase.getProcedureStore().getNumFeatures());
        assertEquals(0, mainObsDatabase.getProcedureStore().getNumRecords());
        assertEquals(0, mainObsDatabase.getFoiStore().getNumFeatures());
        assertEquals(0, mainObsDatabase.getFoiStore().getNumRecords());
        assertEquals(0, mainObsDatabase.getObservationStore().getNumRecords());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().getNumRecords());
        
        assertTrue(mainObsDatabase.getProcedureStore().isEmpty());
        assertTrue(mainObsDatabase.getFoiStore().isEmpty());
        assertTrue(mainObsDatabase.getObservationStore().isEmpty());
        assertTrue(mainObsDatabase.getObservationStore().getDataStreams().isEmpty());
        
        assertEquals(0, mainObsDatabase.getProcedureStore().keySet().size());
        assertEquals(0, mainObsDatabase.getFoiStore().keySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().keySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().keySet().size());
        
        assertEquals(0, mainObsDatabase.getProcedureStore().entrySet().size());
        assertEquals(0, mainObsDatabase.getFoiStore().entrySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().entrySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().entrySet().size());
        
        assertEquals(0, mainObsDatabase.getProcedureStore().values().size());
        assertEquals(0, mainObsDatabase.getFoiStore().values().size());
        assertEquals(0, mainObsDatabase.getObservationStore().values().size());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().values().size());
        
        assertEquals(0, mainObsDatabase.getProcedureStore().selectEntries(new FeatureFilter.Builder().build()).count());
        assertEquals(0, mainObsDatabase.getFoiStore().selectEntries(new FeatureFilter.Builder().build()).count());
        assertEquals(0, mainObsDatabase.getObservationStore().selectEntries(new ObsFilter.Builder().build()).count());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().selectEntries(new DataStreamFilter.Builder().build()).count());
        
        assertEquals(0, mainObsDatabase.getProcedureStore().getAllFeatureIDs().count());
        assertEquals(0, mainObsDatabase.getFoiStore().getAllFeatureIDs().count());
    }
    
    
    @Test
    public void testPublicKeys()
    {
        IHistoricalObsDatabase db1 = new InMemoryProcedureStateDatabase((byte)1);
        IHistoricalObsDatabase db2 = new InMemoryProcedureStateDatabase((byte)2);
        registry.register(Sets.newHashSet("sensor1", "sensor2"), db1);
        registry.register(Sets.newHashSet("sensor3", "sensor4"), db2);
        
        AbstractProcess proc1 = new SimpleProcessImpl();
        proc1.setUniqueIdentifier("sensor1");
        FeatureKey fk1 = db1.getProcedureStore().add(proc1);
    
        AbstractProcess proc3 = new SimpleProcessImpl();
        proc3.setUniqueIdentifier("sensor3");
        FeatureKey fk3 = db2.getProcedureStore().add(proc3);
        
        FeatureId id1 = mainObsDatabase.getProcedureStore().getFeatureID(FeatureKey.builder()
            .withUniqueID("sensor1")
            .build());
        assertTrue(fk1.getInternalID()*DefaultDatabaseRegistry.MAX_NUM_DB+db1.getDatabaseID() == id1.getInternalID());
        
        FeatureId id3 = mainObsDatabase.getProcedureStore().getFeatureID(FeatureKey.builder()
            .withUniqueID("sensor3")
            .build());
        assertTrue(fk3.getInternalID()*DefaultDatabaseRegistry.MAX_NUM_DB+db2.getDatabaseID() == id3.getInternalID());
        
        assertEquals(2, mainObsDatabase.getProcedureStore().keySet().size());        
    }

}
