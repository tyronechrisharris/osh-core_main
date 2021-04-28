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
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.procedure.ProcedureId;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEHelper;
import org.vast.swe.SWEUtils;
import org.vast.util.TimeExtent;
import com.google.common.collect.Lists;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Abstract base for testing implementations of ICommandStreamStore.
 * </p>
 *
 * @author Alex Robin
 * @param <StoreType> Type of store under test
 * @since Mar 28, 2021
 */
public abstract class AbstractTestCommandStreamStore<StoreType extends ICommandStreamStore>
{
    protected static String PROC_UID_PREFIX = "urn:osh:test:sensor:";
    
    protected StoreType cmdStreamStore;
    protected Map<CommandStreamKey, ICommandStreamInfo> allCmdStreams = new LinkedHashMap<>();
    protected Map<CommandStreamKey, ICommandStreamInfo> expectedResults = new LinkedHashMap<>();

    protected abstract StoreType initStore() throws Exception;
    protected abstract void forceReadBackFromStorage() throws Exception;


    @Before
    public void init() throws Exception
    {
        this.cmdStreamStore = initStore();        
    }
    

    protected CommandStreamKey addCommandStream(ProcedureId procID, DataComponent recordStruct, TimeExtent validTime) throws DataStoreException
    {
        var builder = new CommandStreamInfo.Builder()
            .withProcedure(procID)
            .withRecordDescription(recordStruct)
            .withRecordEncoding(new TextEncodingImpl());
        
        if (validTime != null)
            builder.withValidTime(validTime);
                
        var dsInfo = builder.build();
        var key = cmdStreamStore.add(dsInfo);
        allCmdStreams.put(key, dsInfo);
        return key;
    }
    
    
    protected void addToExpectedResults(Entry<CommandStreamKey, ICommandStreamInfo> entry)
    {
        expectedResults.put(entry.getKey(), entry.getValue());
    }
    
    
    protected void addToExpectedResults(int... entryIdxList)
    {
        for (int idx: entryIdxList)
        {
            var entryList = Lists.newArrayList(allCmdStreams.entrySet());
            addToExpectedResults(entryList.get(idx));
        }
    }


    protected CommandStreamKey addSimpleCommandStream(ProcedureId procID, String outputName, String description, TimeExtent validTime) throws DataStoreException
    {
        SWEHelper fac = new SWEHelper();
        var dataStruct = fac.createRecord()
            .name(outputName)
            .description(description)
            .addField("t1", fac.createTime().asSamplingTimeIsoUTC().build())
            .addField("q2", fac.createQuantity().build())
            .addField("c3", fac.createCount().build())
            .addField("b4", fac.createBoolean().build())
            .addField("txt5", fac.createText().build())
            .build();
        
        return addCommandStream(procID, dataStruct, validTime);
    }
    
    
    protected CommandStreamKey addSimpleCommandStream(long procID, String outputName, TimeExtent validTime) throws DataStoreException
    {
        return addSimpleCommandStream(new ProcedureId(procID, PROC_UID_PREFIX+procID), outputName, "command stream description", validTime);
    }
    
    
    protected CommandStreamKey addSimpleCommandStream(long procID, String outputName, String description, TimeExtent validTime) throws DataStoreException
    {
        return addSimpleCommandStream(new ProcedureId(procID, PROC_UID_PREFIX+procID), outputName, description, validTime);
    }


    protected void checkCommandStreamEqual(ICommandStreamInfo ds1, ICommandStreamInfo ds2)
    {
        assertEquals(ds1.getProcedureID(), ds2.getProcedureID());
        assertEquals(ds1.getControlInputName(), ds2.getControlInputName());
        assertEquals(ds1.getName(), ds2.getName());
        assertEquals(ds1.getDescription(), ds2.getDescription());
        checkDataComponentEquals(ds1.getRecordStructure(), ds2.getRecordStructure());
        //assertEquals(ds1.getRecordEncoding(), ds2.getRecordEncoding());
        assertEquals(ds1.getValidTime(), ds2.getValidTime());
    }


    protected void checkDataComponentEquals(DataComponent c1, DataComponent c2)
    {
        SWEUtils utils = new SWEUtils(SWEUtils.V2_0);
        ByteArrayOutputStream os1 = new ByteArrayOutputStream();
        ByteArrayOutputStream os2 = new ByteArrayOutputStream();
        
        try
        {
            utils.writeComponent(os1, c1, false, false);
            utils.writeComponent(os2, c2, false, false);
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }        

        assertArrayEquals(os1.toByteArray(), os2.toByteArray());
    }


    protected void checkSelectedEntries(Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream, Map<CommandStreamKey, ICommandStreamInfo> expectedResults, CommandStreamFilter filter)
    {
        System.out.println("Select command streams with " + filter);
        checkSelectedEntries(resultStream, expectedResults);
    }
    
    
    protected void checkSelectedEntries(Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream, Map<CommandStreamKey, ICommandStreamInfo> expectedResults)
    {
        Map<CommandStreamKey, ICommandStreamInfo> resultMap = resultStream
                //.peek(e -> System.out.println(e.getKey()))
                //.peek(e -> System.out.println(Arrays.toString((double[])e.getValue().getResult().getUnderlyingObject())))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        System.out.println(resultMap.size() + " entries selected");
        
        resultMap.forEach((k, v) -> {
            assertTrue("Result set contains extra key "+k, expectedResults.containsKey(k));
            checkCommandStreamEqual(expectedResults.get(k), v);
        });

        expectedResults.forEach((k, v) -> {
            assertTrue("Result set is missing key "+k, resultMap.containsKey(k));
        });
    }


    @Test
    public void testAddAndGetByKey() throws Exception
    {
        // add N different command streams
        var now = TimeExtent.beginAt(Instant.now());
        int numDs = 100;
        for (int i = 1; i < numDs; i++)
        {
            long procID = i;
            addSimpleCommandStream(procID, "test1", now);
        }
        
        // read back and check
        forceReadBackFromStorage();
        for (Entry<CommandStreamKey, ICommandStreamInfo> entry: allCmdStreams.entrySet())
        {
            ICommandStreamInfo dsInfo = cmdStreamStore.get(entry.getKey());
            assertEquals(entry.getValue().getProcedureID(), dsInfo.getProcedureID());
            assertEquals(entry.getValue().getControlInputName(), dsInfo.getControlInputName());
            checkDataComponentEquals(entry.getValue().getRecordStructure(), dsInfo.getRecordStructure());
        }
    }


    @Test
    public void testGetWrongKey() throws Exception
    {
        assertNull(cmdStreamStore.get(new CommandStreamKey(1L)));
        assertNull(cmdStreamStore.get(new CommandStreamKey(21L)));
        
        // add N different command streams
        var idList = new ArrayList<Long>();
        var now = TimeExtent.beginAt(Instant.now());
        for (int i = 1; i < 5; i++)
        {
            long procID = i;
            var k = addSimpleCommandStream(procID, "test1", now);
            idList.add(k.getInternalID());
        }
        
        assertNotNull(cmdStreamStore.get(new CommandStreamKey(idList.get(0))));
        assertNull(cmdStreamStore.get(new CommandStreamKey(21L)));
        forceReadBackFromStorage();
        assertNull(cmdStreamStore.get(new CommandStreamKey(11L)));
        assertNotNull(cmdStreamStore.get(new CommandStreamKey(idList.get(3))));
        
    }


    private void checkMapKeySet(Set<CommandStreamKey> keySet)
    {
        keySet.forEach(k -> {
            if (!allCmdStreams.containsKey(k))
                fail("No matching key in reference list: " + k);
        });

        allCmdStreams.keySet().forEach(k -> {
            if (!keySet.contains(k))
                fail("No matching key in datastore: " + k);
        });
    }


    private void checkMapValues(Collection<ICommandStreamInfo> mapValues)
    {
        mapValues.forEach(ds -> {
            boolean found = false;
            for (ICommandStreamInfo truth: allCmdStreams.values()) {
                try { checkCommandStreamEqual(ds, truth); found = true; break; }
                catch (Throwable e) {}
            }
            if (!found)
                fail("Invalid command stream: " + ds);
        });
    }


    @Test
    public void testAddAndCheckMapKeysAndValues() throws Exception
    {
        // add N different command streams
        var now = TimeExtent.beginAt(Instant.now());
        int numDs = 56;
        for (int i = numDs; i < numDs*2; i++)
        {
            long procID = i;
            addSimpleCommandStream(procID, "out" + (int)(Math.random()*10), now);
        }
        
        // read back and check
        forceReadBackFromStorage();
        checkMapKeySet(cmdStreamStore.keySet());
        checkMapValues(cmdStreamStore.values());
    }


    @Test
    public void testAddAndRemoveByKey() throws Exception
    {
        // add N different command streams
        var now = TimeExtent.beginAt(Instant.now());
        int numDs = 56;
        for (int i = numDs; i < numDs*2; i++)
        {
            long procID = i;
            addSimpleCommandStream(procID, "out" + (int)(Math.random()*10), now);
        }
        
        assertEquals(numDs, cmdStreamStore.getNumRecords());
        
        int i = 0;
        for (var id: allCmdStreams.keySet())
        {
            var ds = cmdStreamStore.remove(id);
            checkCommandStreamEqual(allCmdStreams.get(id), ds);            
            
            if (i % 5 == 0)
                forceReadBackFromStorage();
            
            i++;
            assertEquals(numDs-i, cmdStreamStore.getNumRecords());
        }
        
        // check that there is nothing left
        assertEquals(0, cmdStreamStore.getNumRecords());
    }


    @Test
    public void testAddAndRemoveByFilter() throws Exception
    {
        // add N different command streams
        var idList = new ArrayList<Long>();
        var now = TimeExtent.beginAt(Instant.now());
        int numDs = 45;
        for (int i = 1; i <= numDs; i++)
        {
            long procID = i;
            var key = addSimpleCommandStream(procID, "out"+i, now);
            idList.add(key.getInternalID());
        }
        
        int numRecords = numDs;
        assertEquals(numRecords, cmdStreamStore.getNumRecords());
        
        // remove some by ID
        var removedIds = new long[] {idList.get(3), idList.get(15), idList.get(36), idList.get(24)};
        for (long id: removedIds)
            allCmdStreams.remove(new CommandStreamKey(id));
        cmdStreamStore.removeEntries(new CommandStreamFilter.Builder()
                .withInternalIDs(removedIds)
                .build());
        checkSelectedEntries(cmdStreamStore.entrySet().stream(), allCmdStreams);
        numRecords -= removedIds.length;
        assertEquals(numRecords, cmdStreamStore.getNumRecords());
        
        // remove some by name
        var removedIdsList = Arrays.asList(idList.get(4), idList.get(41), idList.get(29), idList.get(11));
        var removedNames = removedIdsList.stream()
            .map(id -> allCmdStreams.get(new CommandStreamKey(id)).getControlInputName())
            .collect(Collectors.toList());
        for (long id: removedIdsList)
            allCmdStreams.remove(new CommandStreamKey(id));
        cmdStreamStore.removeEntries(new CommandStreamFilter.Builder()
                .withControlInputNames(removedNames)
                .build());
        checkSelectedEntries(cmdStreamStore.entrySet().stream(), allCmdStreams);
        numRecords -= removedIdsList.size();
        assertEquals(numRecords, cmdStreamStore.getNumRecords());
        
        // remove the rest
        cmdStreamStore.removeEntries(new CommandStreamFilter.Builder()
            .build());
        assertEquals(0, cmdStreamStore.getNumRecords());
    }
    
    
    @Test
    @SuppressWarnings("unused")
    public void testAddAndSelectCurrentVersion() throws Exception
    {
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;

        long procID = 10;
        var now = Instant.now();
        var ds1v0 = addSimpleCommandStream(procID, "test1", TimeExtent.beginAt(now.minusSeconds(3600)));
        var ds1v1 = addSimpleCommandStream(procID, "test1", TimeExtent.beginAt(now.minusSeconds(1200)));
        var ds1v2 = addSimpleCommandStream(procID, "test1", TimeExtent.beginAt(now));
        var ds2v0 = addSimpleCommandStream(procID, "test2", TimeExtent.beginAt(now.minusSeconds(3600)));
        var ds2v1 = addSimpleCommandStream(procID, "test2", TimeExtent.beginAt(now));
        var ds2v2 = addSimpleCommandStream(procID, "test2", TimeExtent.beginAt(now.plusSeconds(600)));
        var ds3v0 = addSimpleCommandStream(procID, "test3", TimeExtent.beginAt(now.minusSeconds(3600)));
        var ds3v1 = addSimpleCommandStream(procID, "test3", TimeExtent.beginAt(now.minusSeconds(600)));
        forceReadBackFromStorage();

        // last version of everything
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
            .withProcedures(procID)
            .withCurrentVersion()
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        testAddAndSelectCurrentVersion_ExpectedResults();
        checkSelectedEntries(resultStream, expectedResults, filter);
    }
    
    
    protected void testAddAndSelectCurrentVersion_ExpectedResults()
    {
        addToExpectedResults(2, 4, 7);
    }
        
    
    @Test
    @SuppressWarnings("unused")
    public void testAddAndSelectLatestValidTime() throws Exception
    {
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;

        long procID = 1;
        var now = Instant.now();
        var ds1v0 = addSimpleCommandStream(procID, "test1", TimeExtent.beginAt(now.minusSeconds(3600)));
        var ds1v1 = addSimpleCommandStream(procID, "test1", TimeExtent.beginAt(now.minusSeconds(1200)));
        var ds1v2 = addSimpleCommandStream(procID, "test1", TimeExtent.beginAt(now));
        var ds2v0 = addSimpleCommandStream(procID, "test2", TimeExtent.beginAt(now.minusSeconds(3600)));
        var ds2v1 = addSimpleCommandStream(procID, "test2", TimeExtent.beginAt(now));
        var ds2v2 = addSimpleCommandStream(procID, "test2", TimeExtent.beginAt(now.plusSeconds(600)));
        var ds3v0 = addSimpleCommandStream(procID, "test3", TimeExtent.beginAt(now.minusSeconds(3600)));
        var ds3v1 = addSimpleCommandStream(procID, "test3", TimeExtent.beginAt(now.minusSeconds(600)));
        forceReadBackFromStorage();

        // last version of everything
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
            .withProcedures(procID)
            .withValidTime(new TemporalFilter.Builder()
                .withLatestTime()
                .build())
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        testAddAndSelectLatestValidTime_ExpectedResults();
        checkSelectedEntries(resultStream, expectedResults, filter);
    }
    
    
    protected void testAddAndSelectLatestValidTime_ExpectedResults()
    {
        addToExpectedResults(2, 5, 7);
    }
    
    
    @Test
    @SuppressWarnings("unused")
    public void testAddAndSelectByTimeRange() throws Exception
    {
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;
        
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var ds1v0 = addSimpleCommandStream(1, "out1", TimeExtent.beginAt(now.minus(365, ChronoUnit.DAYS)));
        var ds1v1 = addSimpleCommandStream(1, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds2v0 = addSimpleCommandStream(1, "out2", TimeExtent.beginAt(now.minus(520, ChronoUnit.DAYS)));
        var ds2v1 = addSimpleCommandStream(1, "out2", TimeExtent.beginAt(now.minus(10, ChronoUnit.DAYS)));        
        var ds3v0 = addSimpleCommandStream(1, "out3", TimeExtent.beginAt(now.minus(30, ChronoUnit.DAYS)));
        var ds3v1 = addSimpleCommandStream(1, "out3", TimeExtent.beginAt(now.minus(1, ChronoUnit.DAYS)));
        var ds4v0 = addSimpleCommandStream(3, "temp", TimeExtent.beginAt(now.plus(1, ChronoUnit.DAYS)));
        var ds5v0 = addSimpleCommandStream(3, "hum", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        
        // select from t0 to now
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
            .withValidTimeDuring(now.minus(10, ChronoUnit.DAYS), now)
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        testAddAndSelectByTimeRange_ExpectedResults(1);
        checkSelectedEntries(resultStream, expectedResults, filter);
                
        // select from t0 to t1
        forceReadBackFromStorage();
        filter = new CommandStreamFilter.Builder()
            .withValidTimeDuring(now.minus(90, ChronoUnit.DAYS), now.minus(30, ChronoUnit.DAYS))
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByTimeRange_ExpectedResults(2);
        checkSelectedEntries(resultStream, expectedResults, filter);
        
        // select from t0 to t1, only proc 3
        forceReadBackFromStorage();
        filter = new CommandStreamFilter.Builder()
            .withProcedures(3)
            .withValidTimeDuring(now.minus(90, ChronoUnit.DAYS), now.minus(30, ChronoUnit.DAYS))
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByTimeRange_ExpectedResults(3);
        checkSelectedEntries(resultStream, expectedResults, filter); 
    }
    
    
    protected void testAddAndSelectByTimeRange_ExpectedResults(int testCaseIdx)
    {
        switch (testCaseIdx)
        {
            case 1: addToExpectedResults(1, 3, 4, 5, 7); break;
            case 2: addToExpectedResults(0, 1, 2, 4, 7); break;
            case 3: addToExpectedResults(7); break;
        }        
    }
    
    
    @Test
    @SuppressWarnings("unused")
    public void testAddAndSelectByControlInputName() throws Exception
    {
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;
        
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var ds1v0 = addSimpleCommandStream(1, "out1", TimeExtent.beginAt(now.minus(365, ChronoUnit.DAYS)));
        var ds1v1 = addSimpleCommandStream(1, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds2v0 = addSimpleCommandStream(1, "out2", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds3v0 = addSimpleCommandStream(2, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds4v0 = addSimpleCommandStream(3, "temp", TimeExtent.beginAt(now.plus(1, ChronoUnit.DAYS)));
        var ds5v0 = addSimpleCommandStream(3, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        
        // select from t0 to now
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
            .withControlInputNames("out1")
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        testAddAndSelectByOutputName_ExpectedResults();
        checkSelectedEntries(resultStream, expectedResults, filter);
    }
    
    
    protected void testAddAndSelectByOutputName_ExpectedResults()
    {
        addToExpectedResults(0, 1, 3, 5);
    }
    
    
    @Test
    @SuppressWarnings("unused")
    public void testAddAndSelectByProcedureID() throws Exception
    {
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;
        
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var ds1v0 = addSimpleCommandStream(1, "out1", TimeExtent.beginAt(now.minus(365, ChronoUnit.DAYS)));
        var ds1v1 = addSimpleCommandStream(1, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds2v0 = addSimpleCommandStream(1, "out2", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds3v0 = addSimpleCommandStream(2, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds4v0 = addSimpleCommandStream(3, "temp", TimeExtent.beginAt(now.plus(1, ChronoUnit.DAYS)));
        var ds5v0 = addSimpleCommandStream(3, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        
        // select from t0 to now
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
            .withProcedures(2, 3)
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        testAddAndSelectByProcedureID_ExpectedResults();
        checkSelectedEntries(resultStream, expectedResults, filter);
    }
    
    
    protected void testAddAndSelectByProcedureID_ExpectedResults()
    {
        addToExpectedResults(3, 4, 5);
    }
    
    
    @Test
    @SuppressWarnings("unused")
    public void testAddAndSelectByKeywords() throws Exception
    {
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;
        CommandStreamFilter filter;
        
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        var ds1v0 = addSimpleCommandStream(1, "out1", "Stationary weather data", TimeExtent.beginAt(now.minus(365, ChronoUnit.DAYS)));
        var ds1v1 = addSimpleCommandStream(1, "out1", "Stationary weather data", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds2v0 = addSimpleCommandStream(1, "out2", "Traffic video stream", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds3v0 = addSimpleCommandStream(2, "out1", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        var ds4v0 = addSimpleCommandStream(3, "temp", "Air temperature", TimeExtent.beginAt(now.plus(1, ChronoUnit.DAYS)));
        var ds5v0 = addSimpleCommandStream(3, "out1", "Air pressure", TimeExtent.beginAt(now.minus(60, ChronoUnit.DAYS)));
        
        // select with one keyword
        filter = new CommandStreamFilter.Builder()
            .withKeywords("air")
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByKeywords_ExpectedResults(1);
        checkSelectedEntries(resultStream, expectedResults, filter);
        
        // select with 2 keywords
        filter = new CommandStreamFilter.Builder()
            .withKeywords("air", "weather")
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByKeywords_ExpectedResults(2);
        checkSelectedEntries(resultStream, expectedResults, filter);
        
        // select with 2 keywords
        filter = new CommandStreamFilter.Builder()
            .withKeywords("air", "video")
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByKeywords_ExpectedResults(3);
        checkSelectedEntries(resultStream, expectedResults, filter);
        
        // select with procedure and keywords (partial words)
        filter = new CommandStreamFilter.Builder()
            .withProcedures(3)
            .withKeywords("weather", "temp")
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByKeywords_ExpectedResults(4);
        checkSelectedEntries(resultStream, expectedResults, filter);
        
        // select unknown keywords
        filter = new CommandStreamFilter.Builder()
            .withProcedures(3)
            .withKeywords("lidar", "humidity")
            .build();
        resultStream = cmdStreamStore.selectEntries(filter);
        
        expectedResults.clear();
        testAddAndSelectByKeywords_ExpectedResults(5);
        checkSelectedEntries(resultStream, expectedResults, filter);
    }
    
    
    protected void testAddAndSelectByKeywords_ExpectedResults(int testCaseIdx)
    {
        switch (testCaseIdx)
        {
            case 1: addToExpectedResults(4, 5); break;
            case 2: addToExpectedResults(0, 1, 4, 5); break;
            case 3: addToExpectedResults(2, 4, 5); break;
            case 4: addToExpectedResults(4); break;
        }        
    }
    
    
    @Test(expected = DataStoreException.class)
    public void testErrorAddWithExistingInput() throws Exception
    {
        var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        addSimpleCommandStream(1, "cmd1", TimeExtent.beginAt(now));
        addSimpleCommandStream(1, "cmd1", TimeExtent.beginAt(now));
    }
    
    
    @Test(expected = IllegalStateException.class)
    public void testErrorWithProcedureFilterJoin() throws Exception
    {
        try
        {
            cmdStreamStore.selectEntries(new CommandStreamFilter.Builder()
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
}
