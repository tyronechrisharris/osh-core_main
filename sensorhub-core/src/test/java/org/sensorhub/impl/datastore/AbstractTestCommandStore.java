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
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.system.SystemId;
import org.sensorhub.api.command.CommandStreamInfo;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.command.ICommandAck.CommandStatusCode;
import org.sensorhub.api.command.ICommandAck;
import org.sensorhub.api.command.CommandAck;
import org.sensorhub.api.command.CommandData;
import org.vast.data.DataBlockDouble;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEHelper;
import com.google.common.collect.Maps;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Abstract base for testing implementations of ICommandStore.
 * </p>
 *
 * @author Alex Robin
 * @param <StoreType> Type of store under test
 * @since Mar 26, 2021
 */
public abstract class AbstractTestCommandStore<StoreType extends ICommandStore>
{
    protected static String CMD_DATASTORE_NAME = "test-cmd";
    protected static String PROC_DATASTORE_NAME = "test-proc";
    protected static String PROC_UID_PREFIX = "urn:osh:test:sensor:";
    protected static String FOI_UID_PREFIX = "urn:osh:test:foi:";
    
    protected StoreType cmdStore;
    protected Map<CommandStreamKey, ICommandStreamInfo> allCommandStreams = new LinkedHashMap<>();
    protected Map<BigInteger, ICommandAck> allCommands = new LinkedHashMap<>();


    protected abstract StoreType initStore() throws Exception;
    protected abstract void forceReadBackFromStorage() throws Exception;


    @Before
    public void init() throws Exception
    {
        this.cmdStore = initStore();
        
    }


    protected CommandStreamKey addCommandStream(long sysID, DataComponent recordStruct)
    {
        try
        {
            var csInfo = new CommandStreamInfo.Builder()
                .withName(recordStruct.getName())
                .withSystem(new SystemId(sysID, PROC_UID_PREFIX+sysID))
                .withRecordDescription(recordStruct)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
            
            var csID = cmdStore.getCommandStreams().add(csInfo);
            allCommandStreams.put(csID, csInfo);
            return csID;
        }
        catch (DataStoreException e)
        {
            throw new IllegalStateException(e);
        }
    }


    protected CommandStreamKey addSimpleCommandStream(long sysID, String outputName)
    {
        SWEHelper fac = new SWEHelper();
        var builder = fac.createRecord()
            .name(outputName);
        for (int i=0; i<5; i++)
            builder.addField("comp"+i, fac.createQuantity().build());        
        return addCommandStream(sysID, builder.build());
    }


    protected BigInteger addCommand(ICommandAck cmd)
    {
        BigInteger key = cmdStore.add(cmd);
        allCommands.put(key, cmd);
        return key;
    }


    protected void addCommands(long csID, Instant startTime, int numObs) throws Exception
    {
        addCommands(csID, startTime, numObs, 60000, true);
    }
    
    
    protected Map<BigInteger, ICommandAck> addCommands(long csID, Instant startTime, int numCmds, long timeStepMillis, Boolean success) throws Exception
    {
        Map<BigInteger, ICommandAck> addedCmds = new LinkedHashMap<>();
        
        for (int i = 0; i < numCmds; i++)
        {
            DataBlockDouble data = new DataBlockDouble(5);
            for (int s=0; s<5; s++)
                data.setDoubleValue(s, i+s);

            var ts = startTime.plusMillis(timeStepMillis*i);
            var cmd = new CommandData.Builder()
                .withCommandStream(csID)
                .withIssueTime(ts)
                .withParams(data)
                .build();

            var successFlag = (success == null) ? i % 2 == 0 : success;
            var cmdAck = successFlag ? CommandAck.success(cmd, ts) : CommandAck.fail(cmd);
            BigInteger key = addCommand(cmdAck);
            addedCmds.put(key, cmdAck);
        }

        System.out.println("Inserted " + numCmds + " commands " +
            "with commandStreamID=" + csID + " starting on " + startTime);

        return addedCmds;
    }


    private void checkCommandDataWithAckEqual(ICommandAck o1, ICommandAck o2)
    {
        assertEquals(o1.getCommandStreamID(), o2.getCommandStreamID());
        assertEquals(o1.getSenderID(), o2.getSenderID());
        assertEquals(o1.getIssueTime(), o2.getIssueTime());
        assertEquals(o1.getActuationTime(), o2.getActuationTime());
        assertEquals(o1.getStatusCode(), o2.getStatusCode());
        assertEquals(o1.getError(), o2.getError());
        assertEquals(o1.getParams().getClass(), o2.getParams().getClass());
        assertEquals(o1.getParams().getAtomCount(), o2.getParams().getAtomCount());
    }


    @Test
    public void testGetDatastoreName() throws Exception
    {
        assertEquals(CMD_DATASTORE_NAME, cmdStore.getDatastoreName());

        forceReadBackFromStorage();
        assertEquals(CMD_DATASTORE_NAME, cmdStore.getDatastoreName());
    }


    protected void checkGetCommands(int expectedNumObs) throws Exception
    {
        assertEquals(expectedNumObs, cmdStore.getNumRecords());

        for (Entry<BigInteger, ICommandAck> entry: allCommands.entrySet())
        {
            ICommandAck cmd = cmdStore.get(entry.getKey());
            checkCommandDataWithAckEqual(cmd, entry.getValue());
        }
    }


    @Test
    public void testGetNumRecordsOneDataStream() throws Exception
    {
        int totalObs = 0, numObs;
        var csKey = addSimpleCommandStream(10, "out1");
        
        // add cmd w/o FOI
        addCommands(csKey.getInternalID(), Instant.parse("2000-01-01T00:00:00Z"), numObs=100);
        assertEquals(totalObs += numObs, cmdStore.getNumRecords());

        forceReadBackFromStorage();
        assertEquals(totalObs, cmdStore.getNumRecords());
    }


    @Test
    public void testGetNumRecordsTwoDataStreams() throws Exception
    {
        int totalObs = 0, numObs;
        var cs1 = addSimpleCommandStream(1, "out1");
        var cs2 = addSimpleCommandStream(2, "out1");

        // add cmd with proc1
        addCommands(cs1.getInternalID(), Instant.parse("2000-06-21T14:36:12Z"), numObs=100);
        assertEquals(totalObs += numObs, cmdStore.getNumRecords());

        // add cmd with proc2
        addCommands(cs2.getInternalID(), Instant.parse("1970-01-01T00:00:00Z"), numObs=50);
        assertEquals(totalObs += numObs, cmdStore.getNumRecords());

        forceReadBackFromStorage();
        assertEquals(totalObs, cmdStore.getNumRecords());
    }


    @Test
    public void testAddAndGetByKeyOneDataStream() throws Exception
    {
        int totalObs = 0, numObs;
        var csKey = addSimpleCommandStream(1, "out1");
        
        // add cmd w/o FOI
        addCommands(csKey.getInternalID(), Instant.parse("2000-01-01T00:00:00Z"), numObs=100);
        checkGetCommands(totalObs += numObs);
        forceReadBackFromStorage();
        checkGetCommands(totalObs);

        // add cmd with FOI
        addCommands(csKey.getInternalID(), Instant.parse("9080-02-01T00:00:00Z"), numObs=30);
        checkGetCommands(totalObs += numObs);
        forceReadBackFromStorage();
        checkGetCommands(totalObs);
    }


    @Test
    public void testGetWrongKey() throws Exception
    {
        testGetNumRecordsOneDataStream();
        assertNull(cmdStore.get(BigInteger.valueOf(11)));
    }


    protected void checkMapKeySet(Set<BigInteger> keySet)
    {
        keySet.forEach(k -> {
            if (!allCommands.containsKey(k))
                fail("No matching key in reference list: " + k);
        });

        allCommands.keySet().forEach(k -> {
            if (!keySet.contains(k))
                fail("No matching key in datastore: " + k);
        });
    }


    @Test
    public void testAddAndCheckMapKeys() throws Exception
    {
        var csKey = addSimpleCommandStream(10, "out1");
        addCommands(csKey.getInternalID(), Instant.parse("2000-01-01T00:00:00Z"), 100);
        checkMapKeySet(cmdStore.keySet());

        forceReadBackFromStorage();
        checkMapKeySet(cmdStore.keySet());

        csKey = addSimpleCommandStream(10, "out2");
        addCommands(csKey.getInternalID(), Instant.MIN.plusSeconds(1), 11);
        addCommands(csKey.getInternalID(), Instant.MAX.minus(10, ChronoUnit.DAYS), 11);
        checkMapKeySet(cmdStore.keySet());

        csKey = addSimpleCommandStream(456, "output");
        addCommands(csKey.getInternalID(), Instant.parse("1950-01-01T00:00:00.5648712Z"), 56);
        checkMapKeySet(cmdStore.keySet());

        forceReadBackFromStorage();
        checkMapKeySet(cmdStore.keySet());
    }


    private void checkMapValues(Collection<ICommandAck> mapValues)
    {
        mapValues.forEach(obs -> {
            boolean found = false;
            for (ICommandAck truth: allCommands.values()) {
                try { checkCommandDataWithAckEqual(obs, truth); found = true; break; }
                catch (Throwable e) {}
            }
            if (!found)
                fail();
        });
    }


    @Test
    public void testAddAndCheckMapValues() throws Exception
    {
        var csKey = addSimpleCommandStream(100, "output3");
        
        addCommands(csKey.getInternalID(), Instant.parse("1900-01-01T00:00:00Z"), 100);
        checkMapValues(cmdStore.values());

        forceReadBackFromStorage();
        checkMapValues(cmdStore.values());
    }


    protected void checkRemoveAllKeys()
    {
        assertTrue(cmdStore.getNumRecords() == allCommands.size());

        long t0 = System.currentTimeMillis();
        allCommands.forEach((k, f) -> {
            cmdStore.remove(k);
            assertFalse(cmdStore.containsKey(k));
            assertTrue(cmdStore.get(k) == null);
        });
        cmdStore.commit();
        System.out.println(String.format("%d cmd removed in %d ms", allCommands.size(), System.currentTimeMillis()-t0));

        assertTrue(cmdStore.isEmpty());
        assertTrue(cmdStore.getNumRecords() == 0);
        allCommands.clear();
    }


    @Test
    public void testAddAndRemoveByKey() throws Exception
    {
        var csKey = addSimpleCommandStream(10, "out1");
        
        addCommands(csKey.getInternalID(), Instant.parse("1900-01-01T00:00:00Z"), 100);
        checkRemoveAllKeys();

        addCommands(csKey.getInternalID(), Instant.parse("2900-01-01T00:00:00Z"), 100);
        forceReadBackFromStorage();
        checkRemoveAllKeys();

        forceReadBackFromStorage();
        addCommands(csKey.getInternalID(), Instant.parse("0001-01-01T00:00:00Z"), 100);
        checkRemoveAllKeys();

        forceReadBackFromStorage();
        checkRemoveAllKeys();
    }


    protected void checkSelectedEntries(Stream<Entry<BigInteger, ICommandAck>> resultStream, Map<BigInteger, ICommandAck> expectedResults, CommandFilter filter)
    {
        System.out.println("Select cmd with " + filter);

        Map<BigInteger, ICommandAck> resultMap = resultStream
                //.peek(e -> System.out.println(e.getKey()))
                //.peek(e -> System.out.println(Arrays.toString((double[])e.getValue().getResult().getUnderlyingObject())))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        System.out.println(resultMap.size() + " entries selected");
        assertEquals(expectedResults.size(), resultMap.size());
        
        resultMap.forEach((k, v) -> {
            assertTrue("Result set contains extra key "+k, expectedResults.containsKey(k));
            checkCommandDataWithAckEqual(expectedResults.get(k), v);
        });

        expectedResults.forEach((k, v) -> {
            assertTrue("Result set is missing key "+k, resultMap.containsKey(k));
        });
    }


    @Test
    public void testSelectCommandsByDataStreamIDAndTime() throws Exception
    {
        Stream<Entry<BigInteger, ICommandAck>> resultStream;
        CommandFilter filter;

        var csKey = addSimpleCommandStream(10, "out1");
        Instant startTime1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, ICommandAck> cmdBatch1 = addCommands(csKey.getInternalID(), startTime1, 55, 1000, true);
        Instant startTime2 = Instant.parse("2019-05-31T10:46:03.258Z");
        Map<BigInteger, ICommandAck> cmdBatch2 = addCommands(csKey.getInternalID(), startTime2, 100, 10000, false);

        // correct stream ID and all times
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .build();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, allCommands, filter);

        // correct stream ID and issue time range containing all
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .withIssueTimeDuring(startTime1, startTime2.plus(1, ChronoUnit.DAYS))
            .build();
        forceReadBackFromStorage();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, allCommands, filter);

        // correct stream ID and actuation time range containing all
        // this should select only successful commands since failed ones have no actuation time
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .withActuationTimeDuring(startTime1, startTime2.plus(1, ChronoUnit.DAYS))
            .build();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, cmdBatch1, filter);

        // correct stream ID and issue time range containing only batch 1
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .withIssueTimeDuring(startTime1, startTime1.plus(1, ChronoUnit.DAYS))
            .build();
        forceReadBackFromStorage();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, cmdBatch1, filter);

        // correct stream ID and issue time range containing only batch 2
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .withIssueTimeDuring(startTime2, startTime2.plus(1, ChronoUnit.DAYS))
            .build();
        forceReadBackFromStorage();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, cmdBatch2, filter);

        // correct stream ID and latest time
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .withLatestIssued()
            .build();
        forceReadBackFromStorage();
        resultStream = cmdStore.selectEntries(filter);
        var lastTimeStamp = startTime2.plusMillis(99*10000);
        checkSelectedEntries(resultStream, Maps.filterValues(cmdBatch2,
            v -> v.getIssueTime().equals(lastTimeStamp)), filter);

        // incorrect stream ID
        filter = new CommandFilter.Builder()
            .withCommandStreams(12L)
            .build();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, Collections.emptyMap(), filter);

        // incorrect time range
        filter = new CommandFilter.Builder()
            .withCommandStreams(csKey.getInternalID())
            .withActuationTimeDuring(startTime1.minus(100, ChronoUnit.DAYS), startTime1.minusMillis(1))
            .build();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, Collections.emptyMap(), filter);
    }


    @Test
    public void testSelectCommandsByDataStreamIDAndPredicates() throws Exception
    {
        Stream<Entry<BigInteger, ICommandAck>> resultStream;
        Map<BigInteger, ICommandAck> expectedResults;
        CommandFilter filter;

        var cs1 = addSimpleCommandStream(1, "out1");
        Instant startTime1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, ICommandAck> cmdBatch1 = addCommands(cs1.getInternalID(), startTime1, 55, 1000, null);
        Instant startTime2 = Instant.parse("2019-05-31T10:46:03.258Z");
        Map<BigInteger, ICommandAck> cmdBatch2 = addCommands(cs1.getInternalID(), startTime2, 100, 10000, true);

        var cs2 = addSimpleCommandStream(1, "out2");
        Instant startProc2Batch1 = Instant.parse("2018-02-11T08:12:06.897Z");
        addCommands(cs2.getInternalID(), startProc2Batch1, 10, 10*24*3600*1000L, true);

        // command stream 1 and predicate to select by status code
        filter = new CommandFilter.Builder()
            .withCommandStreams(cs1.getInternalID(), cs2.getInternalID())
            .withValuePredicate(v -> v.getStatusCode() == CommandStatusCode.SUCCESS)
            .build();
        resultStream = cmdStore.selectEntries(filter);
        expectedResults = Maps.filterValues(allCommands, v -> v.getStatusCode() == CommandStatusCode.SUCCESS);
        checkSelectedEntries(resultStream, expectedResults, filter);

        // command stream 1 and predicate to select param < 10
        filter = new CommandFilter.Builder()
            .withCommandStreams(cs1.getInternalID())
            .withValuePredicate(v -> v.getParams().getDoubleValue(0) < 10)
            .build();
        resultStream = cmdStore.selectEntries(filter);
        expectedResults = new LinkedHashMap<>();
        var finalExpectedResults = expectedResults;
        cmdBatch1.entrySet().stream().limit(10).forEach(e -> finalExpectedResults.put(e.getKey(), e.getValue()));
        cmdBatch2.entrySet().stream().limit(10).forEach(e -> finalExpectedResults.put(e.getKey(), e.getValue()));        
        checkSelectedEntries(resultStream, finalExpectedResults, filter);
    }


    @Test
    public void testSelectCommandsByCommandStreamFilter() throws Exception
    {
        Stream<Entry<BigInteger, ICommandAck>> resultStream;
        Map<BigInteger, ICommandAck> expectedResults = new LinkedHashMap<>();
        CommandFilter filter;

        var cs1 = addSimpleCommandStream(1, "test1");
        var cs2 = addSimpleCommandStream(1, "test2");

        Instant startBatch1 = Instant.parse("2018-02-11T08:12:06.897Z");
        Map<BigInteger, ICommandAck> cmdBatch1 = addCommands(cs1.getInternalID(), startBatch1, 55, 1000, true);

        Instant startBatch2 = Instant.parse("2018-02-11T08:11:48.125Z");
        Map<BigInteger, ICommandAck> cmdBatch2 = addCommands(cs2.getInternalID(), startBatch2, 10, 1200, false);

        // command stream 2 by ID
        filter = new CommandFilter.Builder()
            .withCommandStreams(cs2.getInternalID())
            .build();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, cmdBatch2, filter);

        // command stream 1 & 2 by proc ID
        filter = new CommandFilter.Builder()
            .withSystems(1L)
            .build();
        resultStream = cmdStore.selectEntries(filter);
        expectedResults.clear();
        expectedResults.putAll(cmdBatch1);
        expectedResults.putAll(cmdBatch2);
        checkSelectedEntries(resultStream, expectedResults, filter);

        // command stream 1 by control input name
        forceReadBackFromStorage();
        filter = new CommandFilter.Builder()
            .withCommandStreams(new CommandStreamFilter.Builder()
                .withControlInputNames("test1")
                .build())
            .build();
        resultStream = cmdStore.selectEntries(filter);
        checkSelectedEntries(resultStream, cmdBatch1, filter);
    }
    
    
    @Test(expected = IllegalStateException.class)
    public void testErrorWithSystemFilterJoin() throws Exception
    {
        try
        {
            cmdStore.selectEntries(new CommandFilter.Builder()
                .withSystems()
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
