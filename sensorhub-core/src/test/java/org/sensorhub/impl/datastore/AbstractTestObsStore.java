/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore;

import static org.junit.Assert.*;
import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.ObsData;
import org.sensorhub.api.procedure.ProcedureId;
import org.vast.data.DataBlockDouble;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEHelper;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Abstract base for testing implementations of IObsStore.
 * </p>
 *
 * @author Alex Robin
 * @param <StoreType> Type of store under test
 * @since Apr 14, 2018
 */
public abstract class AbstractTestObsStore<StoreType extends IObsStore>
{
    protected static String OBS_DATASTORE_NAME = "test-obs";
    protected static String PROC_DATASTORE_NAME = "test-proc";
    protected static String FOI_DATASTORE_NAME = "test-foi";
    protected static String PROC_UID_PREFIX = "urn:osh:test:sensor:";
    protected static String FOI_UID_PREFIX = "urn:osh:test:foi:";
    
    protected StoreType obsStore;
    protected Map<DataStreamKey, IDataStreamInfo> allDataStreams = new LinkedHashMap<>();
    protected Map<BigInteger, IObsData> allObs = new LinkedHashMap<>();


    protected abstract StoreType initStore() throws Exception;
    protected abstract void forceReadBackFromStorage() throws Exception;


    @Before
    public void init() throws Exception
    {
        this.obsStore = initStore();
        
    }


    protected DataStreamKey addDataStream(long procID, DataComponent recordStruct)
    {
        try
        {
            var dsInfo = new DataStreamInfo.Builder()
                .withProcedure(new ProcedureId(procID, PROC_UID_PREFIX+procID))
                .withRecordDescription(recordStruct)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
            
            var dsID = obsStore.getDataStreams().add(dsInfo);
            allDataStreams.put(dsID, dsInfo);
            return dsID;
        }
        catch (DataStoreException e)
        {
            throw new IllegalStateException(e);
        }
    }


    protected DataStreamKey addSimpleDataStream(long procID, String outputName)
    {
        SWEHelper fac = new SWEHelper();
        var builder = fac.createRecord()
            .name(outputName);
        for (int i=0; i<5; i++)
            builder.addField("comp"+i, fac.createQuantity().build());        
        return addDataStream(procID, builder.build());
    }


    protected BigInteger addObservation(ObsData obs)
    {
        BigInteger key = obsStore.add(obs);
        allObs.put(key, obs);
        return key;
    }


    protected void addSimpleObsWithoutResultTime(long dsID, long foiID, Instant startTime, int numObs) throws Exception
    {
        addSimpleObsWithoutResultTime(dsID, foiID, startTime, numObs, 60000);
    }


    protected Map<BigInteger, IObsData> addSimpleObsWithoutResultTime(long dsID, long foiID, Instant startTime, int numObs, long timeStepMillis) throws Exception
    {
        Map<BigInteger, IObsData> addedObs = new LinkedHashMap<>();
        FeatureId foi = foiID == 0 ? IObsData.NO_FOI : new FeatureId(foiID, FOI_UID_PREFIX + foiID);
        
        for (int i = 0; i < numObs; i++)
        {
            DataBlockDouble data = new DataBlockDouble(5);
            for (int s=0; s<5; s++)
                data.setDoubleValue(s, i+s);

            ObsData obs = new ObsData.Builder()
                .withDataStream(dsID)
                .withFoi(foi)
                .withPhenomenonTime(startTime.plusMillis(timeStepMillis*i))
                .withResult(data)
                .build();

            BigInteger key = addObservation(obs);
            addedObs.put(key, obs);
        }

        System.out.println("Inserted " + numObs + " observations " +
            "with dataStreamID=" + dsID + ", foiID=" + foiID + " starting on " + startTime);

        return addedObs;
    }


    private void checkObsDataEqual(IObsData o1, IObsData o2)
    {
        assertEquals(o1.getDataStreamID(), o2.getDataStreamID());
        assertEquals(o1.getFoiID(), o2.getFoiID());
        assertEquals(o1.getResultTime(), o2.getResultTime());
        assertEquals(o1.getPhenomenonTime(), o2.getPhenomenonTime());
        assertEquals(o1.getParameters(), o2.getParameters());
        assertEquals(o1.getPhenomenonLocation(), o2.getPhenomenonLocation());
        assertEquals(o1.getResult().getClass(), o2.getResult().getClass());
        assertEquals(o1.getResult().getAtomCount(), o2.getResult().getAtomCount());
    }


    @Test
    public void testGetDatastoreName() throws Exception
    {
        assertEquals(OBS_DATASTORE_NAME, obsStore.getDatastoreName());

        forceReadBackFromStorage();
        assertEquals(OBS_DATASTORE_NAME, obsStore.getDatastoreName());
    }


    protected void checkGetObs(int expectedNumObs) throws Exception
    {
        assertEquals(expectedNumObs, obsStore.getNumRecords());

        for (Entry<BigInteger, IObsData> entry: allObs.entrySet())
        {
            IObsData obs = obsStore.get(entry.getKey());
            checkObsDataEqual(obs, entry.getValue());
        }
    }


    @Test
    public void testGetNumRecordsOneDataStream() throws Exception
    {
        int totalObs = 0, numObs;
        var dsKey = addSimpleDataStream(10, "out1");
        
        // add obs w/o FOI
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.parse("2000-01-01T00:00:00Z"), numObs=100);
        assertEquals(totalObs += numObs, obsStore.getNumRecords());

        forceReadBackFromStorage();
        assertEquals(totalObs, obsStore.getNumRecords());
    }


    @Test
    public void testGetNumRecordsTwoDataStreams() throws Exception
    {
        int totalObs = 0, numObs;
        var ds1 = addSimpleDataStream(1, "out1");
        var ds2 = addSimpleDataStream(2, "out1");

        // add obs with proc1
        addSimpleObsWithoutResultTime(ds1.getInternalID(), 0, Instant.parse("2000-06-21T14:36:12Z"), numObs=100);
        assertEquals(totalObs += numObs, obsStore.getNumRecords());

        // add obs with proc2
        addSimpleObsWithoutResultTime(ds2.getInternalID(), 0, Instant.parse("1970-01-01T00:00:00Z"), numObs=50);
        assertEquals(totalObs += numObs, obsStore.getNumRecords());

        forceReadBackFromStorage();
        assertEquals(totalObs, obsStore.getNumRecords());
    }


    @Test
    public void testAddAndGetByKeyOneDataStream() throws Exception
    {
        int totalObs = 0, numObs;
        var dsKey = addSimpleDataStream(1, "out1");
        
        // add obs w/o FOI
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.parse("2000-01-01T00:00:00Z"), numObs=100);
        checkGetObs(totalObs += numObs);
        forceReadBackFromStorage();
        checkGetObs(totalObs);

        // add obs with FOI
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 1001, Instant.parse("9080-02-01T00:00:00Z"), numObs=30);
        checkGetObs(totalObs += numObs);
        forceReadBackFromStorage();
        checkGetObs(totalObs);
    }


    @Test
    public void testGetWrongKey() throws Exception
    {
        testGetNumRecordsOneDataStream();
        assertNull(obsStore.get(BigInteger.valueOf(11)));
    }


    protected void checkMapKeySet(Set<BigInteger> keySet)
    {
        keySet.forEach(k -> {
            if (!allObs.containsKey(k))
                fail("No matching key in reference list: " + k);
        });

        allObs.keySet().forEach(k -> {
            if (!keySet.contains(k))
                fail("No matching key in datastore: " + k);
        });
    }


    @Test
    public void testAddAndCheckMapKeys() throws Exception
    {
        var dsKey = addSimpleDataStream(10, "out1");
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.parse("2000-01-01T00:00:00Z"), 100);
        checkMapKeySet(obsStore.keySet());

        forceReadBackFromStorage();
        checkMapKeySet(obsStore.keySet());

        dsKey = addSimpleDataStream(10, "out2");
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.MIN.plusSeconds(1), 11);
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.MAX.minus(10, ChronoUnit.DAYS), 11);
        checkMapKeySet(obsStore.keySet());

        dsKey = addSimpleDataStream(456, "output");
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 569, Instant.parse("1950-01-01T00:00:00.5648712Z"), 56);
        checkMapKeySet(obsStore.keySet());

        forceReadBackFromStorage();
        checkMapKeySet(obsStore.keySet());
    }


    private void checkMapValues(Collection<IObsData> mapValues)
    {
        mapValues.forEach(obs -> {
            boolean found = false;
            for (IObsData truth: allObs.values()) {
                try { checkObsDataEqual(obs, truth); found = true; break; }
                catch (Throwable e) {}
            }
            if (!found)
                fail();
        });
    }


    @Test
    public void testAddAndCheckMapValues() throws Exception
    {
        var dsKey = addSimpleDataStream(100, "output3");
        
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.parse("1900-01-01T00:00:00Z"), 100);
        checkMapValues(obsStore.values());

        forceReadBackFromStorage();
        checkMapValues(obsStore.values());
    }


    protected void checkRemoveAllKeys()
    {
        assertTrue(obsStore.getNumRecords() == allObs.size());

        long t0 = System.currentTimeMillis();
        allObs.forEach((k, f) -> {
            obsStore.remove(k);
            assertFalse(obsStore.containsKey(k));
            assertTrue(obsStore.get(k) == null);
        });
        obsStore.commit();
        System.out.println(String.format("%d obs removed in %d ms", allObs.size(), System.currentTimeMillis()-t0));

        assertTrue(obsStore.isEmpty());
        assertTrue(obsStore.getNumRecords() == 0);
        allObs.clear();
    }


    @Test
    public void testAddAndRemoveByKey() throws Exception
    {
        var dsKey = addSimpleDataStream(10, "out1");
        
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, Instant.parse("1900-01-01T00:00:00Z"), 100);
        checkRemoveAllKeys();

        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 563, Instant.parse("2900-01-01T00:00:00Z"), 100);
        forceReadBackFromStorage();
        checkRemoveAllKeys();

        forceReadBackFromStorage();
        addSimpleObsWithoutResultTime(dsKey.getInternalID(), 1003, Instant.parse("0001-01-01T00:00:00Z"), 100);
        checkRemoveAllKeys();

        forceReadBackFromStorage();
        checkRemoveAllKeys();
    }


    protected void checkSelectedEntries(Stream<Entry<BigInteger, IObsData>> resultStream, Map<BigInteger, IObsData> expectedResults, ObsFilter filter)
    {
        System.out.println("Select obs with " + filter);

        Map<BigInteger, IObsData> resultMap = resultStream
                //.peek(e -> System.out.println(e.getKey()))
                //.peek(e -> System.out.println(Arrays.toString((double[])e.getValue().getResult().getUnderlyingObject())))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        assertEquals(expectedResults.size(), resultMap.size());
        System.out.println(resultMap.size() + " entries selected");

        resultMap.forEach((k, v) -> {
            assertTrue("Result set contains extra key "+k, expectedResults.containsKey(k));
            checkObsDataEqual(expectedResults.get(k), v);
        });

        expectedResults.forEach((k, v) -> {
            assertTrue("Result set is missing key "+k, resultMap.containsKey(k));
        });
    }


    @Test
    public void testSelectObsByDataStreamIDAndTime() throws Exception
    {
        Stream<Entry<BigInteger, IObsData>> resultStream;
        ObsFilter filter;

        var dsKey = addSimpleDataStream(10, "out1");
        Instant startTime1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, IObsData> obsBatch1 = addSimpleObsWithoutResultTime(dsKey.getInternalID(), 0, startTime1, 55, 1000);
        Instant startTime2 = Instant.parse("2019-05-31T10:46:03.258Z");
        Map<BigInteger, IObsData> obsBatch2 = addSimpleObsWithoutResultTime(dsKey.getInternalID(), 104, startTime2, 100, 10000);

        // correct procedure ID and all times
        filter = new ObsFilter.Builder()
            .withDataStreams(dsKey.getInternalID())
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, allObs, filter);

        // correct procedure ID and time range containing all
        filter = new ObsFilter.Builder()
            .withDataStreams(dsKey.getInternalID())
            .withPhenomenonTimeDuring(startTime1, startTime2.plus(1, ChronoUnit.DAYS))
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, allObs, filter);

        // correct procedure ID and time range containing only batch 1
        filter = new ObsFilter.Builder()
            .withDataStreams(dsKey.getInternalID())
            .withPhenomenonTimeDuring(startTime1, startTime1.plus(1, ChronoUnit.DAYS))
            .build();
        forceReadBackFromStorage();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, obsBatch1, filter);

        // correct procedure ID and time range containing only batch 2
        filter = new ObsFilter.Builder()
            .withDataStreams(dsKey.getInternalID())
            .withPhenomenonTimeDuring(startTime2, startTime2.plus(1, ChronoUnit.DAYS))
            .build();
        forceReadBackFromStorage();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, obsBatch2, filter);

        // incorrect procedure ID
        filter = new ObsFilter.Builder()
            .withDataStreams(12L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, Collections.emptyMap(), filter);

        // incorrect time range
        filter = new ObsFilter.Builder()
            .withDataStreams(dsKey.getInternalID())
            .withPhenomenonTimeDuring(startTime1.minus(100, ChronoUnit.DAYS), startTime1.minusMillis(1))
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, Collections.emptyMap(), filter);
    }


    @Test
    public void testSelectObsByDataStreamIDAndFoiID() throws Exception
    {
        Stream<Entry<BigInteger, IObsData>> resultStream;
        Map<BigInteger, IObsData> expectedResults;
        ObsFilter filter;

        var ds1 = addSimpleDataStream(1, "out1");
        Instant startProc1Batch1 = Instant.parse("2015-06-23T18:24:15.233Z");
        Map<BigInteger, IObsData> proc1Batch1 = addSimpleObsWithoutResultTime(ds1.getInternalID(), 23, startProc1Batch1, 10, 30*24*3600*1000L);
        Instant startProc1Batch2 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, IObsData> proc1Batch2 = addSimpleObsWithoutResultTime(ds1.getInternalID(), 46, startProc1Batch2, 3, 100*24*3600*1000L);
        Instant startProc1Batch3 = Instant.parse("2025-06-23T18:24:15.233Z");
        Map<BigInteger, IObsData> proc1Batch3 = addSimpleObsWithoutResultTime(ds1.getInternalID(), 0, startProc1Batch3, 10, 30*24*3600*1000L);

        var ds2 = addSimpleDataStream(1, "out2");
        Instant startProc2Batch1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, IObsData> proc2Batch1 = addSimpleObsWithoutResultTime(ds2.getInternalID(), 23, startProc2Batch1, 10, 10*24*3600*1000L);
        Instant startProc2Batch2 = Instant.parse("2019-05-31T10:46:03.258Z");
        Map<BigInteger, IObsData> proc2Batch2 = addSimpleObsWithoutResultTime(ds2.getInternalID(), 104, startProc2Batch2, 100, 24*3600*1000L);
        Instant startProc2Batch3 = Instant.parse("2020-05-31T10:46:03.258Z");
        Map<BigInteger, IObsData> proc2Batch3 = addSimpleObsWithoutResultTime(ds2.getInternalID(), 104, startProc2Batch3, 50, 24*3600*1000L);

        // proc1 and all times
        filter = new ObsFilter.Builder()
            .withDataStreams(ds1.getInternalID())
            .build();
        resultStream = obsStore.selectEntries(filter);
        expectedResults = new LinkedHashMap<>();
        expectedResults.putAll(proc1Batch1);
        expectedResults.putAll(proc1Batch2);
        expectedResults.putAll(proc1Batch3);
        checkSelectedEntries(resultStream, expectedResults, filter);

        // proc1, foi46 and all times
        filter = new ObsFilter.Builder()
            .withDataStreams(ds1.getInternalID())
            .withFois(46L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, proc1Batch2, filter);

        // proc1, no foi
        filter = new ObsFilter.Builder()
            .withDataStreams(ds1.getInternalID())
            .withFois(0L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, proc1Batch3, filter);

        // proc2, foi23 and all times
        filter = new ObsFilter.Builder()
            .withDataStreams(ds2.getInternalID())
            .withFois(23L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, proc2Batch1, filter);

        // proc2, foi23 and all times
        filter = new ObsFilter.Builder()
            .withDataStreams(ds2.getInternalID())
            .withFois(23L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, proc2Batch1, filter);

        // proc2, foi104 and time range
        filter = new ObsFilter.Builder()
            .withDataStreams(ds2.getInternalID())
            .withFois(104L)
            .withPhenomenonTimeDuring(startProc2Batch3, startProc2Batch3.plus(49, ChronoUnit.DAYS))
            .build();
        forceReadBackFromStorage();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, proc2Batch3, filter);

        // foi23 from all proc
        filter = new ObsFilter.Builder()
            .withFois(23L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        expectedResults = new LinkedHashMap<>();
        expectedResults.putAll(proc1Batch1);
        expectedResults.putAll(proc2Batch1);
        checkSelectedEntries(resultStream, expectedResults, filter);

        // foi23 and 104 from all proc
        filter = new ObsFilter.Builder()
            .withFois(23L, 104L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        expectedResults = new LinkedHashMap<>();
        expectedResults.putAll(proc1Batch1);
        expectedResults.putAll(proc2Batch1);
        expectedResults.putAll(proc2Batch2);
        expectedResults.putAll(proc2Batch3);
        checkSelectedEntries(resultStream, expectedResults, filter);
    }


    @Test
    public void testSelectObsByDataStreamIDAndPredicates() throws Exception
    {
        Stream<Entry<BigInteger, IObsData>> resultStream;
        Map<BigInteger, IObsData> expectedResults;
        ObsFilter filter;

        var ds1 = addSimpleDataStream(1, "out1");
        Instant startTime1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, IObsData> obsBatch1 = addSimpleObsWithoutResultTime(ds1.getInternalID(), 0, startTime1, 55, 1000);
        Instant startTime2 = Instant.parse("2019-05-31T10:46:03.258Z");
        Map<BigInteger, IObsData> obsBatch2 = addSimpleObsWithoutResultTime(ds1.getInternalID(), 104, startTime2, 100, 10000);

        var ds2 = addSimpleDataStream(1, "out2");
        Instant startProc2Batch1 = Instant.parse("2018-02-11T08:12:06.897Z");
        addSimpleObsWithoutResultTime(ds2.getInternalID(), 23, startProc2Batch1, 10, 10*24*3600*1000L);

        // proc1 and predicate to select NO FOI
        filter = new ObsFilter.Builder()
            .withDataStreams(ds1.getInternalID())
            .withValuePredicate(v -> v.getFoiID() == IObsData.NO_FOI)
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, obsBatch1, filter);

        // proc1 and predicate to select results < 10
        filter = new ObsFilter.Builder()
            .withDataStreams(ds1.getInternalID())
            .withValuePredicate(v -> v.getResult().getDoubleValue(0) < 10)
            .build();
        resultStream = obsStore.selectEntries(filter);
        expectedResults = new LinkedHashMap<>();
        obsBatch1.entrySet().stream().limit(10).forEach(e -> expectedResults.put(e.getKey(), e.getValue()));
        obsBatch2.entrySet().stream().limit(10).forEach(e -> expectedResults.put(e.getKey(), e.getValue()));
        checkSelectedEntries(resultStream, expectedResults, filter);
    }


    @Test
    public void testSelectObsByDataStreamFilter() throws Exception
    {
        Stream<Entry<BigInteger, IObsData>> resultStream;
        Map<BigInteger, IObsData> expectedResults = new LinkedHashMap<>();
        ObsFilter filter;

        var ds1 = addSimpleDataStream(1, "test1");
        var ds2 = addSimpleDataStream(1, "test2");

        Instant startBatch1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, IObsData> obsBatch1 = addSimpleObsWithoutResultTime(ds1.getInternalID(), 0, startBatch1, 55, 1000);

        Instant startBatch2 = Instant.parse("2018-02-11T08:11:48.125Z");
        Map<BigInteger, IObsData> obsBatch2 = addSimpleObsWithoutResultTime(ds2.getInternalID(), 23, startBatch2, 10, 1200);

        // datastream 2 by ID
        filter = new ObsFilter.Builder()
            .withDataStreams(ds2.getInternalID())
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, obsBatch2, filter);

        // datastream 1 & 2 by proc ID
        filter = new ObsFilter.Builder()
            .withProcedures(1L)
            .build();
        resultStream = obsStore.selectEntries(filter);
        expectedResults.clear();
        expectedResults.putAll(obsBatch1);
        expectedResults.putAll(obsBatch2);
        checkSelectedEntries(resultStream, expectedResults, filter);

        // datastream 1 by output name
        forceReadBackFromStorage();
        filter = new ObsFilter.Builder()
            .withDataStreams(new DataStreamFilter.Builder()
                .withOutputNames("test1")
                .build())
            .build();
        resultStream = obsStore.selectEntries(filter);
        checkSelectedEntries(resultStream, obsBatch1, filter);
    }
    
    
    @Test(expected = IllegalStateException.class)
    public void testErrorWithFoiFilterJoin() throws Exception
    {
        try
        {
            obsStore.selectEntries(new ObsFilter.Builder()
                .withFois()
                    .withKeywords("road")
                    .done()
                .build());
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            throw e;
        }
    }
    
    
    @Test(expected = IllegalStateException.class)
    public void testErrorWithProcedureFilterJoin() throws Exception
    {
        try
        {
            obsStore.selectEntries(new ObsFilter.Builder()
                .withProcedures()
                    .withKeywords("thermometer")
                    .done()
                .build());
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage());
            throw e;
        }
    }


    /*
    ///////////////////////
    // Performance Tests //
    ///////////////////////

    @Test
    public void testPutThroughput() throws Exception
    {
        System.out.println("Write Throughput (put operations)");

        int numFeatures = 100000;
        long t0 = System.currentTimeMillis();
        //addSamplingPoints2D(0, numFeatures);
        addNonGeoFeatures(0, numFeatures);
        //addTemporalGeoFeatures(0, numFeatures);
        featureStore.sync();
        double dt = System.currentTimeMillis() - t0;
        int throughPut = (int)(numFeatures/dt*1000);
        System.out.println(String.format("Simple Features: %d writes/s", throughPut));
        assertTrue(throughPut > 50000);

        numFeatures = 10000;
        t0 = System.currentTimeMillis();
        addSamplingPoints2D(0, numFeatures);
        featureStore.sync();
        dt = System.currentTimeMillis() - t0;
        throughPut = (int)(numFeatures/dt*1000);
        System.out.println(String.format("Geo Features: %d writes/s", throughPut));
        assertTrue(throughPut > 10000);

        numFeatures = 10000;
        t0 = System.currentTimeMillis();
        addTemporalGeoFeatures(0, numFeatures);
        featureStore.sync();
        dt = System.currentTimeMillis() - t0;
        throughPut = (int)(numFeatures/dt*1000*NUM_TIME_ENTRIES_PER_FEATURE);
        System.out.println(String.format("Spatio-temporal features: %d writes/s", throughPut));
        assertTrue(throughPut > 10000);
    }


    @Test
    public void testGetThroughput() throws Exception
    {
        System.out.println("Read Throughput (get operations)");

        int numFeatures = 100000;
        addNonGeoFeatures(0, numFeatures);

        // sequential reads
        int numReads = numFeatures;
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < numReads; i++)
        {
            String uid = UID_PREFIX + "F" + i;
            FeatureKey key = FeatureKey.builder()
                    .withUniqueID(uid)
                    .build();

            AbstractFeature f = featureStore.get(key);
            assertEquals(uid, f.getUniqueIdentifier());
        }
        double dt = System.currentTimeMillis() - t0;
        int throughPut = (int)(numReads/dt*1000);
        System.out.println(String.format("Sequential Reads: %d reads/s", throughPut));
        assertTrue(throughPut > 100000);

        // random reads
        numReads = 10000;
        t0 = System.currentTimeMillis();
        for (int i = 0; i < numReads; i++)
        {
            String uid = UID_PREFIX + "F" + (int)(Math.random()*(numFeatures-1));
            FeatureKey key = FeatureKey.builder()
                    .withUniqueID(uid)
                    .build();

            AbstractFeature f = featureStore.get(key);
            assertEquals(uid, f.getUniqueIdentifier());
        }
        dt = System.currentTimeMillis() - t0;
        throughPut = (int)(numReads/dt*1000);
        System.out.println(String.format("Random Reads: %d reads/s", throughPut));
        assertTrue(throughPut > 10000);
    }


    @Test
    public void testScanThroughput() throws Exception
    {
        System.out.println("Scan Throughput (cursor iteration)");

        int numFeatures = 100000;
        addNonGeoFeatures(0, numFeatures);

        // warm up
        long count = featureStore.keySet().stream().count();

        // key scan
        long t0 = System.currentTimeMillis();
        count = featureStore.keySet().stream().count();
        double dt = System.currentTimeMillis() - t0;
        int throughPut = (int)(count/dt*1000);
        System.out.println(String.format("Key Scan: %d reads/s", throughPut));
        assertTrue(throughPut > 200000);

        // entry scan
        t0 = System.currentTimeMillis();
        count = featureStore.entrySet().stream().count();
        dt = System.currentTimeMillis() - t0;
        throughPut = (int)(count/dt*1000);
        System.out.println(String.format("Entry Scan: %d reads/s", throughPut));
        assertTrue(throughPut > 200000);
    }


    @Test
    public void testTemporalFilterThroughput() throws Exception
    {
        System.out.println("Temporal Query Throughput (select operation w/ temporal filter)");

        int numFeatures = 20000;
        addTemporalFeatures(0, numFeatures);

        // spatial filter with all features
        Instant date0 = featureStore.keySet().iterator().next().getValidStartTime();
        FeatureFilter filter = new FeatureFilter.Builder()
                .withValidTimeDuring(date0, date0.plus(numFeatures+NUM_TIME_ENTRIES_PER_FEATURE*30*24, ChronoUnit.HOURS))
                .build();

        long t0 = System.currentTimeMillis();
        long count = featureStore.selectEntries(filter).count();//.forEach(entry -> count.incrementAndGet());
        double dt = System.currentTimeMillis() - t0;
        int throughPut = (int)(count/dt*1000);
        System.out.println(String.format("Entry Stream: %d reads/s", throughPut));
        assertTrue(throughPut > 50000);
        assertEquals(numFeatures*NUM_TIME_ENTRIES_PER_FEATURE, count);
    }



    @Test
    public void testSpatialFilterThroughput() throws Exception
    {
        System.out.println("Geo Query Throughput (select operation w/ spatial filter)");

        int numFeatures = 100000;
        addSamplingPoints2D(0, numFeatures);

        // spatial filter with all features
        FeatureFilter filter = new FeatureFilter.Builder()
                .withLocationWithin(featureStore.getFeaturesBbox())
                .build();

        long t0 = System.currentTimeMillis();
        long count = featureStore.selectEntries(filter).count();
        double dt = System.currentTimeMillis() - t0;
        int throughPut = (int)(count/dt*1000);
        System.out.println(String.format("Entry Stream: %d reads/s", throughPut));
        assertTrue(throughPut > 50000);
        assertEquals(numFeatures, count);

        // with geo temporal features
        int numFeatures2 = 20000;
        addTemporalGeoFeatures(1000, numFeatures2);

        // spatial filter with all features
        filter = new FeatureFilter.Builder()
                .withValidTimeDuring(Instant.MIN, Instant.MAX)
                .withLocationWithin(featureStore.getFeaturesBbox())
                .build();

        t0 = System.currentTimeMillis();
        count = featureStore.selectEntries(filter).count();
        dt = System.currentTimeMillis() - t0;
        throughPut = (int)(count/dt*1000);
        System.out.println(String.format("Entry Stream: %d reads/s", throughPut));
        assertTrue(throughPut > 50000);
        assertEquals(numFeatures+numFeatures2*NUM_TIME_ENTRIES_PER_FEATURE, count);
    }



    ///////////////////////
    // Concurrency Tests //
    ///////////////////////

    /*long refTime;
    int numWrittenMetadataObj;
    int numWrittenRecords;
    volatile int numWriteThreadsRunning;

    protected void startWriteRecordsThreads(final ExecutorService exec,
                                            final int numWriteThreads,
                                            final DataComponent recordDef,
                                            final double timeStep,
                                            final int testDurationMs,
                                            final Collection<Throwable> errors)
    {
        numWriteThreadsRunning = numWriteThreads;

        for (int i=0; i<numWriteThreads; i++)
        {
            final int count = i;
            exec.submit(new Runnable() {
                @Override
                public void run()
                {
                    long startTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("Begin Write Records Thread %d @ %dms\n", Thread.currentThread().getId(), startTimeOffset);

                    try
                    {
                        List<DataBlock> dataList = writeRecords(recordDef, count*10000., timeStep, Integer.MAX_VALUE, testDurationMs);
                        synchronized(AbstractTestFeatureStore.this) {
                            numWrittenRecords += dataList.size();
                        }
                    }
                    catch (Throwable e)
                    {
                        errors.add(e);
                        //exec.shutdownNow();
                    }

                    synchronized(AbstractTestFeatureStore.this) {
                        numWriteThreadsRunning--;
                    }

                    long stopTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("End Write Records Thread %d @ %dms\n", Thread.currentThread().getId(), stopTimeOffset);
                }
            });
        }
    }


    protected void startReadRecordsThreads(final ExecutorService exec,
                                           final int numReadThreads,
                                           final DataComponent recordDef,
                                           final double timeStep,
                                           final Collection<Throwable> errors)
    {
        for (int i=0; i<numReadThreads; i++)
        {
            exec.submit(new Runnable() {
                @Override
                public void run()
                {
                    long tid = Thread.currentThread().getId();
                    long startTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("Begin Read Records Thread %d @ %dms\n", tid, startTimeOffset);
                    int readCount = 0;

                    try
                    {
                        while (numWriteThreadsRunning > 0 && !Thread.interrupted())
                        {
                            //System.out.println(numWriteThreadsRunning);
                            double[] timeRange = storage.getRecordsTimeRange(recordDef.getName());
                            if (Double.isNaN(timeRange[0]))
                                continue;
                            //double[] timeRange = new double[] {0.0, 110000.0};

                            //System.out.format("Read Thread %d, Loop %d\n", Thread.currentThread().getId(), j+1);
                            final double begin = timeRange[0] + Math.random() * (timeRange[1] - timeRange[0]);
                            final double end = begin + Math.max(timeStep*100., Math.random() * (timeRange[1] - begin));

                            // prepare filter
                            IDataFilter filter = new DataFilter(recordDef.getName()) {
                                @Override
                                public double[] getTimeStampRange() { return new double[] {begin, end}; }
                            };

                            // retrieve records
                            Iterator<? extends IDataRecord> it = storage.getRecordIterator(filter);

                            // check records time stamps and order
                            //System.out.format("Read Thread %d, [%f-%f]\n", Thread.currentThread().getId(), begin, end);
                            double lastTimeStamp = Double.NEGATIVE_INFINITY;
                            while (it.hasNext())
                            {
                                IDataRecord rec = it.next();
                                double timeStamp = rec.getKey().timeStamp;

                                //System.out.format("Read Thread %d, %f\n", Thread.currentThread().getId(), timeStamp);
                                assertTrue(tid + ": Time steps are not increasing: " + timeStamp + "<" + lastTimeStamp , timeStamp > lastTimeStamp);
                                assertTrue(tid + ": Time stamp lower than begin: " + timeStamp + "<" + begin , timeStamp >= begin);
                                assertTrue(tid + ": Time stamp higher than end: " + timeStamp + ">" + end, timeStamp <= end);
                                lastTimeStamp = timeStamp;
                                readCount++;
                            }

                            Thread.sleep(1);
                        }
                    }
                    catch (Throwable e)
                    {
                        errors.add(e);
                        //exec.shutdownNow();
                    }

                    long stopTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("End Read Records Thread %d @%dms - %d read ops\n", Thread.currentThread().getId(), stopTimeOffset, readCount);
                }
            });
        }
    }


    protected void startWriteMetadataThreads(final ExecutorService exec,
                                             final int numWriteThreads,
                                             final Collection<Throwable> errors)
    {
        for (int i=0; i<numWriteThreads; i++)
        {
            final int startCount = i*1000000;
            exec.submit(new Runnable() {
                @Override
                public void run()
                {
                    long startTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("Begin Write Desc Thread %d @%dms\n", Thread.currentThread().getId(), startTimeOffset);

                    try
                    {
                        int count = startCount;
                        while (numWriteThreadsRunning > 0 && !Thread.interrupted())
                        {
                            // create description
                            //SWEHelper helper = new SWEHelper();
                            SMLFactory smlFac = new SMLFactory();
                            GMLFactory gmlFac = new GMLFactory();

                            PhysicalSystem system = new PhysicalSystemImpl();
                            system.setUniqueIdentifier("TEST" + count++);
                            system.setName("blablabla");
                            system.setDescription("this is the description of my sensor that can be pretty long");

                            IdentifierList identifierList = smlFac.newIdentifierList();
                            system.addIdentification(identifierList);

                            Term term;
                            term = smlFac.newTerm();
                            term.setDefinition(SWEHelper.getPropertyUri("Manufacturer"));
                            term.setLabel("Manufacturer Name");
                            term.setValue("My manufacturer");
                            identifierList.addIdentifier2(term);

                            term = smlFac.newTerm();
                            term.setDefinition(SWEHelper.getPropertyUri("ModelNumber"));
                            term.setLabel("Model Number");
                            term.setValue("SENSOR_2365");
                            identifierList.addIdentifier2(term);

                            term = smlFac.newTerm();
                            term.setDefinition(SWEHelper.getPropertyUri("SerialNumber"));
                            term.setLabel("Serial Number");
                            term.setValue("FZEFZE154618989");
                            identifierList.addIdentifier2(term);

                            // generate unique time stamp
                            TimePosition timePos = gmlFac.newTimePosition(startCount + System.currentTimeMillis()/1000.);
                            TimeInstant validTime = gmlFac.newTimeInstant(timePos);
                            system.addValidTimeAsTimeInstant(validTime);

                            // add to storage
                            storage.storeDataSourceDescription(system);
                            //storage.commit();

                            synchronized(AbstractTestFeatureStore.this) {
                                numWrittenMetadataObj++;
                            }

                            Thread.sleep(5);
                        }
                    }
                    catch (Throwable e)
                    {
                        errors.add(e);
                        //exec.shutdownNow();
                    }

                    long stopTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("End Write Desc Thread %d @%dms\n", Thread.currentThread().getId(), stopTimeOffset);
                }
            });
        }
    }


    protected void startReadMetadataThreads(final ExecutorService exec,
            final int numReadThreads,
            final Collection<Throwable> errors)
    {
        for (int i = 0; i < numReadThreads; i++)
        {
            exec.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    long tid = Thread.currentThread().getId();
                    long startTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("Begin Read Desc Thread %d @ %dms\n", tid, startTimeOffset);
                    int readCount = 0;

                    try
                    {
                        while (numWriteThreadsRunning > 0 && !Thread.interrupted())
                        {
                            AbstractProcess p = storage.getLatestDataSourceDescription();
                            if (p != null)
                                assertTrue("Missing valid time", p.getValidTimeList().size() > 0);
                            readCount++;
                            Thread.sleep(1);
                        }
                    }
                    catch (Throwable e)
                    {
                        errors.add(e);
                        //exec.shutdownNow();
                    }

                    long stopTimeOffset = System.currentTimeMillis() - refTime;
                    System.out.format("End Read Desc Thread %d @%dms - %d read ops\n", Thread.currentThread().getId(), stopTimeOffset, readCount);
                }
            });
        }
    }


    protected void checkForAsyncErrors(Collection<Throwable> errors) throws Throwable
    {
        // report errors
        System.out.println(errors.size() + " error(s)");
        for (Throwable e: errors)
            e.printStackTrace();
        if (!errors.isEmpty())
            throw errors.iterator().next();
    }


    protected void checkRecordsInStorage(final DataComponent recordDef) throws Throwable
    {
        System.out.println(numWrittenRecords + " records written");

        // check number of records
        int recordCount = storage.getNumRecords(recordDef.getName());
        assertEquals("Wrong number of records in storage", numWrittenRecords, recordCount);

        // check number of records returned by iterator
        recordCount = 0;
        Iterator<?> it = storage.getRecordIterator(new DataFilter(recordDef.getName()));
        while (it.hasNext())
        {
            it.next();
            recordCount++;
        }
        assertEquals("Wrong number of records returned by iterator", numWrittenRecords, recordCount);
    }


    protected void checkMetadataInStorage() throws Throwable
    {
        System.out.println(numWrittenMetadataObj + " metadata objects written");

        int descCount = 0;
        List<AbstractProcess> descList = storage.getDataSourceDescriptionHistory(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        for (AbstractProcess desc: descList)
        {
            assertTrue(desc instanceof PhysicalSystem);
            assertEquals("blablabla", desc.getName());
            assertTrue(desc.getUniqueIdentifier().startsWith("TEST"));
            descCount++;
        }
        assertEquals("Wrong number of metadata objects in storage", numWrittenMetadataObj, descCount);

        AbstractProcess desc = storage.getLatestDataSourceDescription();
        assertTrue(desc instanceof PhysicalSystem);
    }


    @Test
    public void testConcurrentWriteRecords() throws Throwable
    {
        final DataComponent recordDef = createDs2();
        ExecutorService exec = Executors.newCachedThreadPool();
        final Collection<Throwable> errors = Collections.synchronizedCollection(new ArrayList<Throwable>());

        int numWriteThreads = 10;
        int testDurationMs = 2000;
        refTime = System.currentTimeMillis();

        startWriteRecordsThreads(exec, numWriteThreads, recordDef, 0.1, testDurationMs, errors);

        exec.shutdown();
        exec.awaitTermination(testDurationMs*2, TimeUnit.MILLISECONDS);

        forceReadBackFromStorage();
        checkRecordsInStorage(recordDef);
        checkForAsyncErrors(errors);
    }


    @Test
    public void testConcurrentWriteMetadata() throws Throwable
    {
        ExecutorService exec = Executors.newCachedThreadPool();
        final Collection<Throwable> errors = Collections.synchronizedCollection(new ArrayList<Throwable>());

        int numWriteThreads = 10;
        int testDurationMs = 2000;
        refTime = System.currentTimeMillis();

        numWriteThreadsRunning = 1;
        startWriteMetadataThreads(exec, numWriteThreads, errors);

        Thread.sleep(testDurationMs);
        numWriteThreadsRunning = 0;

        exec.shutdown();
        exec.awaitTermination(testDurationMs*2, TimeUnit.MILLISECONDS);

        forceReadBackFromStorage();
        checkMetadataInStorage();
        checkForAsyncErrors(errors);
    }


    @Test
    public void testConcurrentWriteThenReadRecords() throws Throwable
    {
        DataComponent recordDef = createDs2();
        ExecutorService exec = Executors.newCachedThreadPool();
        Collection<Throwable> errors = Collections.synchronizedCollection(new ArrayList<Throwable>());

        int numWriteThreads = 10;
        int numReadThreads = 10;
        int testDurationMs = 1000;
        double timeStep = 0.1;
        refTime = System.currentTimeMillis();

        startWriteRecordsThreads(exec, numWriteThreads, recordDef, timeStep, testDurationMs, errors);

        exec.shutdown();
        exec.awaitTermination(testDurationMs*2, TimeUnit.MILLISECONDS);
        exec = Executors.newCachedThreadPool();
        numWriteThreadsRunning = 1;

        checkForAsyncErrors(errors);
        forceReadBackFromStorage();

        errors.clear();
        startReadRecordsThreads(exec, numReadThreads, recordDef, timeStep, errors);

        Thread.sleep(testDurationMs);
        numWriteThreadsRunning = 0; // manually stop reading after sleep period
        exec.shutdown();
        exec.awaitTermination(1000, TimeUnit.MILLISECONDS);

        checkForAsyncErrors(errors);
        checkRecordsInStorage(recordDef);
    }


    @Test
    public void testConcurrentReadWriteRecords() throws Throwable
    {
        final DataComponent recordDef = createDs2();
        final ExecutorService exec = Executors.newCachedThreadPool();
        final Collection<Throwable> errors = Collections.synchronizedCollection(new ArrayList<Throwable>());

        int numWriteThreads = 10;
        int numReadThreads = 10;
        int testDurationMs = 2000;
        double timeStep = 0.1;
        refTime = System.currentTimeMillis();

        startWriteRecordsThreads(exec, numWriteThreads, recordDef, timeStep, testDurationMs, errors);
        startReadRecordsThreads(exec, numReadThreads, recordDef, timeStep, errors);

        exec.shutdown();
        exec.awaitTermination(testDurationMs*200, TimeUnit.MILLISECONDS);

        forceReadBackFromStorage();
        checkForAsyncErrors(errors);
        checkRecordsInStorage(recordDef);
    }*/
}
