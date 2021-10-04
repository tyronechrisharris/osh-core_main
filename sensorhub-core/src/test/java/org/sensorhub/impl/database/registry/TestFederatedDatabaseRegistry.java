/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.registry;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.datastore.mem.InMemorySystemStateDatabase;
import org.sensorhub.impl.system.wrapper.SystemWrapper;
import org.vast.sensorML.SimpleProcessImpl;
import com.google.common.collect.Sets;
import net.opengis.sensorml.v20.AbstractProcess;


public class TestFederatedDatabaseRegistry
{
    IDatabaseRegistry registry;
    IObsSystemDatabase mainObsDatabase;
    
    
    @Rule
    public ExpectedException errors = ExpectedException.none();
    
    
    @Before
    public void setup()
    {
        SensorHub hub = new SensorHub();
        hub.start();
        this.registry = hub.getDatabaseRegistry();
        this.mainObsDatabase = registry.getFederatedObsDatabase();
    }
    
    
    @Test
    public void testRegister()
    {
        registry.register(Sets.newHashSet("sensor001", "sensor002"), new InMemorySystemStateDatabase((byte)1));
        registry.register(Sets.newHashSet("sensor003", "sensor004"), new InMemorySystemStateDatabase((byte)2));
    }
    
    
    @Test
    public void testRegisterDuplicateProcUIDs()
    {
        registry.register(Sets.newHashSet("sensor001", "sensor002"), new InMemorySystemStateDatabase((byte)1));
        errors.expect(IllegalStateException.class);
        registry.register(Sets.newHashSet("sensor002", "sensor004"), new InMemorySystemStateDatabase((byte)2));
    }
    
    
    @Test
    public void testEmptyDatabases()
    {
        registry.register(Sets.newHashSet("sensor001", "sensor002"), new InMemorySystemStateDatabase((byte)3));
        registry.register(Sets.newHashSet("sensor003", "sensor004"), new InMemorySystemStateDatabase((byte)4));
        
        assertEquals(0, mainObsDatabase.getSystemDescStore().getNumFeatures());
        assertEquals(0, mainObsDatabase.getSystemDescStore().getNumRecords());
        assertEquals(0, mainObsDatabase.getFoiStore().getNumFeatures());
        assertEquals(0, mainObsDatabase.getFoiStore().getNumRecords());
        assertEquals(0, mainObsDatabase.getObservationStore().getNumRecords());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().getNumRecords());
        
        assertTrue(mainObsDatabase.getSystemDescStore().isEmpty());
        assertTrue(mainObsDatabase.getFoiStore().isEmpty());
        assertTrue(mainObsDatabase.getObservationStore().isEmpty());
        assertTrue(mainObsDatabase.getObservationStore().getDataStreams().isEmpty());
        
        assertEquals(0, mainObsDatabase.getSystemDescStore().keySet().size());
        assertEquals(0, mainObsDatabase.getFoiStore().keySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().keySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().keySet().size());
        
        assertEquals(0, mainObsDatabase.getSystemDescStore().entrySet().size());
        assertEquals(0, mainObsDatabase.getFoiStore().entrySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().entrySet().size());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().entrySet().size());
        
        assertEquals(0, mainObsDatabase.getSystemDescStore().values().size());
        assertEquals(0, mainObsDatabase.getFoiStore().values().size());
        assertEquals(0, mainObsDatabase.getObservationStore().values().size());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().values().size());
        
        assertEquals(0, mainObsDatabase.getSystemDescStore().selectEntries(new SystemFilter.Builder().build()).count());
        assertEquals(0, mainObsDatabase.getFoiStore().selectEntries(new FoiFilter.Builder().build()).count());
        assertEquals(0, mainObsDatabase.getObservationStore().selectEntries(new ObsFilter.Builder().build()).count());
        assertEquals(0, mainObsDatabase.getObservationStore().getDataStreams().selectEntries(new DataStreamFilter.Builder().build()).count());
    }
    
    
    @Test
    public void testPublicKeys() throws Exception
    {
        IObsSystemDatabase db1 = new InMemorySystemStateDatabase((byte)1);
        IObsSystemDatabase db2 = new InMemorySystemStateDatabase((byte)2);
        registry.register(Sets.newHashSet("sensor001", "sensor002"), db1);
        registry.register(Sets.newHashSet("sensor003", "sensor004"), db2);
        
        AbstractProcess proc1 = new SimpleProcessImpl();
        proc1.setUniqueIdentifier("sensor001");
        FeatureKey fk1 = db1.getSystemDescStore().add(new SystemWrapper(proc1));
    
        AbstractProcess proc3 = new SimpleProcessImpl();
        proc3.setUniqueIdentifier("sensor003");
        FeatureKey fk3 = db2.getSystemDescStore().add(new SystemWrapper(proc3));
        
        FeatureKey id1 = mainObsDatabase.getSystemDescStore().getCurrentVersionKey("sensor001");
        assertTrue(fk1.getInternalID()*DefaultDatabaseRegistry.MAX_NUM_DB+db1.getDatabaseNum() == id1.getInternalID());
        
        FeatureKey id3 = mainObsDatabase.getSystemDescStore().getCurrentVersionKey("sensor003");
        assertTrue(fk3.getInternalID()*DefaultDatabaseRegistry.MAX_NUM_DB+db2.getDatabaseNum() == id3.getInternalID());
        
        assertEquals(2, mainObsDatabase.getSystemDescStore().keySet().size());        
    }

}
