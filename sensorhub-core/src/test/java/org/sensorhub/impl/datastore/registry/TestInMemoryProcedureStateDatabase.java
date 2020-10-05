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

import static org.junit.Assert.assertEquals;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.feature.IFeatureStoreBase;
import org.sensorhub.impl.datastore.AbstractTestFeatureStore;
import org.sensorhub.impl.procedure.InMemoryProcedureStateDatabase;
import org.vast.ogc.gml.ITemporalFeature;


public class TestInMemoryProcedureStateDatabase
{
    protected int NUM_TIME_ENTRIES_PER_FEATURE = 7;
    protected OffsetDateTime FIRST_VERSION_TIME = OffsetDateTime.parse("2020-01-01T00:00:00Z");
    
    TestFeatureStore procedureStoreTests;
    TestFeatureStore foiStoreTests;
    InMemoryProcedureStateDatabase db;    
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    class TestFeatureStore extends AbstractTestFeatureStore
    {
        IFeatureStoreBase store;
        
        TestFeatureStore(IFeatureStoreBase store)
        {
            this.NUM_TIME_ENTRIES_PER_FEATURE = TestInMemoryProcedureStateDatabase.this.NUM_TIME_ENTRIES_PER_FEATURE;
            this.FIRST_VERSION_TIME = TestInMemoryProcedureStateDatabase.this.FIRST_VERSION_TIME; 
            
            try
            {
                this.store = store;
                init();
            }
            catch (Exception e)
            {}
        }        
        
        @Override
        protected IFeatureStoreBase initStore(ZoneOffset timeZone) throws Exception
        {
            return store;
        }

        @Override
        protected void forceReadBackFromStorage() throws Exception
        {            
        }
        
        @Override
        protected void addTemporalFeatures(int startIndex, int numFeatures) throws Exception
        {
            super.addTemporalFeatures(startIndex, numFeatures);
        }
    }
    
    
    @Before
    public void init() throws Exception
    {
        db = new InMemoryProcedureStateDatabase();
        
        // reuse abstract feature tests for procedures and fois
        procedureStoreTests = new TestFeatureStore(db.getProcedureStore());
        foiStoreTests = new TestFeatureStore(db.getFoiStore());
    }
    
    
    @Test
    public void testAddAndGetFois() throws Exception
    {
        foiStoreTests.testAddAndGet();
    }
    
    
    @Test
    public void testAddFoiVersionsAndGet() throws Exception
    {
        int numFeatures = 100;
        foiStoreTests.addTemporalFeatures(0, numFeatures);
        
        assertEquals(numFeatures, db.getFoiStore().size());
        
        // check that we kept only the latest versions
        for (var key: db.getFoiStore().keySet())
        {
            var proc = (ITemporalFeature)db.getFoiStore().get(key);
            OffsetDateTime expectedTime = FIRST_VERSION_TIME.plus((NUM_TIME_ENTRIES_PER_FEATURE-1)*30, ChronoUnit.DAYS).plus(key.getInternalID()-1, ChronoUnit.HOURS);
            assertEquals(expectedTime.toInstant(), proc.getValidTime().begin());
        }
    }
    
    
    @Test
    public void testAddAndGetDatastreams()
    {
        
    }
    
    
    @Test
    public void testAddAndGetObs()
    {
        
    }

}
