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
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
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
    protected Map<DataStreamKey, IDataStreamInfo> allDataStreams = new ConcurrentHashMap<>();
    protected Map<BigInteger, IObsData> allObs = new ConcurrentHashMap<>();


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
                .withName(recordStruct.getName())
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


    protected Map<BigInteger, IObsData> addSimpleObsWithoutResultTime(long dsID, long foiID, Instant startTime, int numObs) throws Exception
    {
        return addSimpleObsWithoutResultTime(dsID, foiID, startTime, numObs, 60000);
    }


    protected Map<BigInteger, IObsData> addSimpleObsWithoutResultTime(long dsID, long foiID, Instant startTime, int numObs, long timeStepMillis) throws Exception
    {
        Map<BigInteger, IObsData> addedObs = new LinkedHashMap<>();
        
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < numObs; i++)
        {
            DataBlockDouble data = new DataBlockDouble(5);
            for (int s=0; s<5; s++)
                data.setDoubleValue(s, i+s);

            ObsData obs = new ObsData.Builder()
                .withDataStream(dsID)
                .withFoi(foiID)
                .withPhenomenonTime(startTime.plusMillis(timeStepMillis*i))
                .withResult(data)
                .build();

            BigInteger key = addObservation(obs);
            addedObs.put(key, obs);
        }
        long t1 = System.currentTimeMillis();
        
        System.out.println("Inserted " + numObs + " observations in " + (t1-t0) + "ms, " +
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
    
    
    @Test
    public void testAddAndRemoveEntireDatastream() throws Exception
    {
        var ds1Key = addSimpleDataStream(10, "out1");
        var ds2Key = addSimpleDataStream(20, "out2");
        var ds3Key = addSimpleDataStream(30, "out3");
        
        addSimpleObsWithoutResultTime(ds1Key.getInternalID(), 13, Instant.parse("2000-01-01T00:00:00Z"), 50);
        addSimpleObsWithoutResultTime(ds2Key.getInternalID(), 12, Instant.parse("2010-05-26T00:00:00Z"), 100);
        addSimpleObsWithoutResultTime(ds3Key.getInternalID(), 89, Instant.parse("2020-08-14T10:00:00Z"), 120);
        
        forceReadBackFromStorage();
        ObsFilter obsFilter;
        
        obsFilter = new ObsFilter.Builder()
            .withDataStreams(ds1Key.getInternalID())
            .build();
        assertTrue(obsStore.countMatchingEntries(obsFilter) > 0);
        
        obsFilter = new ObsFilter.Builder()
            .withDataStreams(ds2Key.getInternalID())
            .build();
        assertTrue(obsStore.countMatchingEntries(obsFilter) > 0);
        
        obsFilter = new ObsFilter.Builder()
            .withDataStreams(ds3Key.getInternalID())
            .build();
        assertTrue(obsStore.countMatchingEntries(obsFilter) > 0);
        
        // remove datastream 2
        obsStore.getDataStreams().remove(ds2Key);
        forceReadBackFromStorage();
        
        obsFilter = new ObsFilter.Builder()
            .withDataStreams(ds1Key.getInternalID())
            .build();
        assertTrue(obsStore.countMatchingEntries(obsFilter) > 0);
        
        obsFilter = new ObsFilter.Builder()
            .withDataStreams(ds2Key.getInternalID())
            .build();
        assertTrue(obsStore.countMatchingEntries(obsFilter) == 0);
        
        obsFilter = new ObsFilter.Builder()
            .withDataStreams(ds3Key.getInternalID())
            .build();
        assertTrue(obsStore.countMatchingEntries(obsFilter) > 0);
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
            .withFois(IObsData.NO_FOI)
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


    ///////////////////////
    // Concurrency Tests //
    ///////////////////////
    
    static class ObsBatch
    {
        long dsID;
        long foiID;
        Instant startTime;
        long timeStepMillis;
        int numObs;
        volatile Map<BigInteger, IObsData> expectedObs;
    }
    
    protected CompletableFuture<?> addObsConcurrent(List<ObsBatch> series)
    {
        var futures = new CompletableFuture[series.size()];
        
        int t = 0;
        for (var s: series)
        {
            var f = CompletableFuture.runAsync(() -> {
                try
                {
                    var addedObs = addSimpleObsWithoutResultTime(s.dsID, s.foiID, s.startTime, s.numObs, s.timeStepMillis);
                    s.expectedObs = addedObs;
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });
            
            futures[t++] = f;
        }
        
        return CompletableFuture.allOf(futures);
    }
    
    
    protected void readAndCheckSeries(List<ObsBatch> series)
    {
        var foiSet = new HashSet<Long>();
        
        for (var s: series)
        {
            foiSet.add(s.foiID);
            System.out.println(s.foiID + ": " + s.expectedObs.size());
            var filter = new ObsFilter.Builder()
                .withDataStreams(s.dsID)
                .withFois(s.foiID)
                .build();
            checkSelectedEntries(obsStore.selectEntries(filter), s.expectedObs, filter);
        }
        
        // check all fois are present
        var numFois = obsStore.selectObservedFois(obsStore.selectAllFilter())
            .peek(foi -> { assertTrue(foiSet.contains(foi)); })
            .count();
        assertEquals(foiSet.size(), numFois);
    }
    
    
    protected void addAndCheckObsConcurrent(List<ObsBatch> series)
    {
        int totalObs = 0;
        for (var s: series)
            totalObs += s.numObs;
        
        int numObs = totalObs;
        long t0 = System.currentTimeMillis();
        addObsConcurrent(series).thenRun(() -> {
            readAndCheckSeries(series);
            
            double dt = System.currentTimeMillis() - t0;
            int throughPut = (int)(numObs/dt*1000);
            System.out.println(String.format("Simple Obs: %d writes/s", throughPut));
            
            try { forceReadBackFromStorage(); }
            catch (Exception e) { throw new RuntimeException(e); }
            
            readAndCheckSeries(series);
        }).join();
    }
    
    
    protected AtomicInteger selectObsConcurrent(List<ObsBatch> series, int numThreads)
    {
        var exec = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        var requestCounter = new AtomicInteger();
        
        for (int t = 0; t < numThreads; t++)
        {
            var r = new Runnable() {
                public void run()
                {
                    // select random series and start/stop times
                    var s = series.get((int)Math.floor(Math.random()*series.size()));
                    var duration = s.timeStepMillis * s.numObs;
                    var begin = s.startTime.plus(Duration.ofMillis((long)(Math.random()*duration)));
                    var end = begin.plus(Duration.ofMillis((long)(Math.random()*duration)));
                    
                    // scan all results
                    obsStore.select(new ObsFilter.Builder()
                        .withDataStreams(s.dsID)
                        .withFois(s.foiID)
                        .withPhenomenonTimeDuring(begin, end)
                        .build()).count();
                    requestCounter.incrementAndGet();
                    
                    exec.schedule(this, 10, TimeUnit.MILLISECONDS);
                }
            };
            
            exec.submit(r);
        }
        
        return requestCounter;
    }
    
    
    @Test
    public void testConcurrentAdd1() throws Exception
    {
        System.out.println();
        System.out.println("Write Throughput (concurrent add operations)");

        // 1 datastream, 1 thread per foi
        var dsKey = addSimpleDataStream(10, "out1");
        int numObs = 100000;
        int numFois = 100;
        var obsSeries = new ArrayList<ObsBatch>(numFois);
        for (int foi = 1; foi <= numFois; foi++)
        {
            var s = new ObsBatch();
            s.dsID = dsKey.getInternalID();
            s.foiID = foi;
            s.startTime = Instant.parse("2000-01-01T00:00:00Z").plus((long)(Math.random()*60), ChronoUnit.SECONDS);
            s.numObs = numObs/numFois;
            obsSeries.add(s);
        }
        addAndCheckObsConcurrent(obsSeries);
    }
    
    
    @Test
    public void testConcurrentAdd2() throws Exception
    {
        // 1 datastream per thread
        int numObs = 100000;
        int numDs = 100;
        var obsSeries = new ArrayList<ObsBatch>(numDs);
        for (int ds = 1; ds <= numDs; ds++)
        {
            var s = new ObsBatch();
            s.dsID = addSimpleDataStream(10, "out" + (ds+1)).getInternalID();
            s.foiID = (long)(Math.random()*10)+1;
            s.startTime = Instant.parse("1980-01-01T01:36:00Z").plus(ds, ChronoUnit.MINUTES);
            s.numObs = numObs/numDs;
            obsSeries.add(s);
        }
        addAndCheckObsConcurrent(obsSeries);
    }


    @Test
    public void testConcurrentReadWrite() throws Throwable
    {
        // 1 datastream per thread
        int numObs = 100000;
        int numDs = 100;
        var obsSeries = new ArrayList<ObsBatch>(numDs);
        for (int ds = 1; ds <= numDs; ds++)
        {
            var s = new ObsBatch();
            s.dsID = addSimpleDataStream(10, "out" + (ds+1)).getInternalID();
            s.foiID = (long)(Math.random()*10)+1;
            s.startTime = Instant.parse("1980-01-01T01:36:00Z").plus(ds, ChronoUnit.MINUTES);
            s.numObs = numObs/numDs;
            obsSeries.add(s);
        }
        
        // start read threads
        var reqCount = selectObsConcurrent(obsSeries, 10);
        
        // wait for end of writes and check datastore content
        long t0 = System.currentTimeMillis();
        addObsConcurrent(obsSeries).thenRun(() -> {
            readAndCheckSeries(obsSeries);
            
            double dt = System.currentTimeMillis() - t0;
            int throughPut = (int)(numObs/dt*1000);
            System.out.println(String.format("Simple Obs: %d writes/s", throughPut));
            System.out.println(String.format("%d requests", reqCount.get()));
            
            try { forceReadBackFromStorage(); }
            catch (Exception e) { throw new RuntimeException(e); }
            
            readAndCheckSeries(obsSeries);
        }).join();
    }
}
