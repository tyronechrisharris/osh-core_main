/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVBTreeMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVVarLongDataType;
import org.h2.mvstore.RangeCursor;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStore.CommandField;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.procedure.IProcedureStore;
import org.sensorhub.impl.datastore.DataStoreUtils;
import org.sensorhub.impl.datastore.command.CommandStreamInfoWrapper;
import org.sensorhub.impl.datastore.h2.H2Utils.Holder;
import org.sensorhub.impl.datastore.h2.index.FullTextIndex;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Command stream Store implementation based on H2 MVStore.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 24, 2021
 */
public class MVCommandStreamStoreImpl implements ICommandStreamStore
{
    private static final String CMDSTREAM_MAP_NAME = "@cmdstreams";
    private static final String CDMSTREAM_PROC_MAP_NAME = "@cmdstreams_proc";
    private static final String CMDSTREAM_FULLTEXT_MAP_NAME = "@cmdstreams_text";

    protected MVStore mvStore;
    protected MVCommandStoreImpl commandStore;
    protected IProcedureStore procedureStore;
    protected IdProvider<ICommandStreamInfo> idProvider;
    
    /*
     * Main index
     */
    protected MVBTreeMap<CommandStreamKey, ICommandStreamInfo> cmdStreamIndex;
    
    /*
     * Procedure/param index
     * Map of {procedure ID, param name, validTime} to commandstream ID
     * Everything is stored in the key with no value (use MVVoidDataType for efficiency)
     */
    protected MVBTreeMap<MVTimeSeriesProcKey, Boolean> cmdStreamByProcIndex;
    
    /*
     * Full text index pointing to main index
     * Key references are parentID/internalID pairs
     */
    protected FullTextIndex<ICommandStreamInfo, Long> fullTextIndex;
    
    
    /*
     * CommandStreamInfo object wrapper used to compute time ranges lazily
     */
    class CommandStreamInfoWithTimeRanges extends CommandStreamInfoWrapper
    {
        Long csID;
        TimeExtent validTime;
        TimeExtent actuationTimeRange;
        TimeExtent issueTimeRange;
                
        CommandStreamInfoWithTimeRanges(Long internalID, ICommandStreamInfo csInfo)
        {
            super(csInfo);
            this.csID = internalID;
        }

        @Override
        public TimeExtent getValidTime()
        {
            if (validTime == null)
            {
                validTime = super.getValidTime();
                
                // if valid time ends at now and there is a more recent version, compute the actual end time
                if (validTime.endsNow())
                {                
                    var procDsKey = new MVTimeSeriesProcKey(
                        getProcedureID().getInternalID(),
                        getControlInputName(),
                        getValidTime().begin());
                    
                    var nextKey = cmdStreamByProcIndex.higherKey(procDsKey);
                    if (nextKey != null &&
                        nextKey.procedureID == procDsKey.internalID &&
                        nextKey.signalName.equals(procDsKey.signalName))
                        validTime = TimeExtent.period(validTime.begin(), Instant.ofEpochSecond(nextKey.validStartTime));
                }
            }
            
            return validTime;
        }     
        
        @Override
        public TimeExtent getActuationTimeRange()
        {
            if (actuationTimeRange == null)
                actuationTimeRange = commandStore.getCommandStreamActuationTimeRange(csID);
            
            return actuationTimeRange;
        }        
        
        @Override
        public TimeExtent getIssueTimeRange()
        {
            if (issueTimeRange == null)
                issueTimeRange = commandStore.getCommandStreamIssueTimeRange(csID);
            
            return issueTimeRange;
        }
    }


    public MVCommandStreamStoreImpl(MVCommandStoreImpl cmdStore, IdProvider<ICommandStreamInfo> idProvider)
    {
        this.commandStore = Asserts.checkNotNull(cmdStore, MVCommandStoreImpl.class);
        this.mvStore = Asserts.checkNotNull(cmdStore.mvStore, MVStore.class);

        // open command stream map
        String mapName = CMDSTREAM_MAP_NAME + ":" + cmdStore.getDatastoreName();
        this.cmdStreamIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<CommandStreamKey, ICommandStreamInfo>()
                .keyType(new MVCommandStreamKeyDataType())
                .valueType(new MVCommandStreamInfoDataType()));

        // procedure index
        mapName = CDMSTREAM_PROC_MAP_NAME + ":" + cmdStore.getDatastoreName();
        this.cmdStreamByProcIndex = mvStore.openMap(mapName, new MVBTreeMap.Builder<MVTimeSeriesProcKey, Boolean>()
                .keyType(new MVTimeSeriesProcKeyDataType())
                .valueType(new MVVoidDataType()));
        
        // full-text index
        mapName = CMDSTREAM_FULLTEXT_MAP_NAME + ":" + cmdStore.getDatastoreName();
        this.fullTextIndex = new FullTextIndex<>(mvStore, mapName, new MVVarLongDataType());

        // ID provider
        this.idProvider = idProvider;
        if (idProvider == null) // use default if nothing is set
        {
            this.idProvider = csInfo -> {
                if (cmdStreamIndex.isEmpty())
                    return 1;
                else
                    return cmdStreamIndex.lastKey().getInternalID()+1;
            };
        }
    }
    
    
    @Override
    public synchronized CommandStreamKey add(ICommandStreamInfo csInfo) throws DataStoreException
    {
        DataStoreUtils.checkCommandStreamInfo(procedureStore, csInfo);
        
        // use valid time of parent procedure or current time if none was set
        csInfo = DataStoreUtils.ensureValidTime(procedureStore, csInfo);

        // create key
        var newKey = generateKey(csInfo);

        // add to store
        put(newKey, csInfo, false);
        return newKey;
    }
    
    
    protected CommandStreamKey generateKey(ICommandStreamInfo csInfo)
    {
        return new CommandStreamKey(idProvider.newInternalID(csInfo));
    }


    @Override
    public ICommandStreamInfo get(Object key)
    {
        var csKey = DataStoreUtils.checkCommandStreamKey(key);
        
        var csInfo = cmdStreamIndex.get(csKey);
        if (csInfo == null)
            return null;
        
        return new CommandStreamInfoWithTimeRanges(csKey.getInternalID(), csInfo);
    }


    Stream<Long> getCommandStreamIdsByProcedure(long procID, Set<String> outputNames, TemporalFilter validTime)
    {
        MVTimeSeriesProcKey first = new MVTimeSeriesProcKey(procID, "", Instant.MIN);
        RangeCursor<MVTimeSeriesProcKey, Boolean> cursor = new RangeCursor<>(cmdStreamByProcIndex, first);

        Stream<MVTimeSeriesProcKey> keyStream = cursor.keyStream()
            .takeWhile(k -> k.procedureID == procID);

        // we post filter output names and versions during the scan
        // since number of outputs and versions is usually small, this should
        // be faster than looking up each output separately
        return postFilterKeyStream(keyStream, outputNames, validTime)
            .map(k -> k.internalID);
    }


    Stream<Long> getCommandStreamIdsFromAllProcedures(Set<String> outputNames, TemporalFilter validTime)
    {
        Stream<MVTimeSeriesProcKey> keyStream = cmdStreamByProcIndex.keySet().stream();

        // yikes we're doing a full index scan here!
        return postFilterKeyStream(keyStream, outputNames, validTime)
            .map(k -> k.internalID);
    }


    Stream<MVTimeSeriesProcKey> postFilterKeyStream(Stream<MVTimeSeriesProcKey> keyStream, Set<String> outputNames, TemporalFilter validTime)
    {
        if (outputNames != null)
            keyStream = keyStream.filter(k -> outputNames.contains(k.signalName));

        if (validTime != null)
        {
            // handle special case of current time & latest time
            long filterStartTime, filterEndTime;
            if (validTime.isCurrentTime()) {
                filterStartTime = filterEndTime = Instant.now().getEpochSecond();
            } else if (validTime.isLatestTime()) {
                filterStartTime = filterEndTime = Instant.MAX.getEpochSecond();
            } else {
                filterStartTime = validTime.getMin().getEpochSecond();
                filterEndTime = validTime.getMax().getEpochSecond();                
            }
            
            // get all datastream with validStartTime within the filter range + 1 before
            // recall that datastreams are ordered in reverse valid time order
            Holder<MVTimeSeriesProcKey> lastKey = new Holder<>();
            keyStream = keyStream
                .filter(k -> {
                    MVTimeSeriesProcKey saveLastKey = lastKey.value;
                    lastKey.value = k;
                                            
                    if (k.validStartTime > filterEndTime)
                        return false;
                    
                    if (k.validStartTime >= filterStartTime)
                        return true;
                    
                    if (saveLastKey == null ||
                        k.procedureID != saveLastKey.procedureID ||
                        !k.signalName.equals(saveLastKey.signalName) ||
                        saveLastKey.validStartTime > filterStartTime) {
                        return true;
                    }
                    
                    return false;
                });
        }

        return keyStream;
    }


    @Override
    public Stream<Entry<CommandStreamKey, ICommandStreamInfo>> selectEntries(CommandStreamFilter filter, Set<CommandStreamInfoField> fields)
    {
        Stream<Long> idStream = null;
        Stream<Entry<CommandStreamKey, ICommandStreamInfo>> resultStream;
        boolean fullTextFilterApplied = false;
        
        // if filtering by internal IDs, use these IDs directly
        if (filter.getInternalIDs() != null)
        {
            idStream = filter.getInternalIDs().stream();
        }

        // if procedure filter is used
        else if (filter.getProcedureFilter() != null)
        {
            // first select procedures and fetch corresponding command streams
            idStream = DataStoreUtils.selectProcedureIDs(procedureStore, filter.getProcedureFilter())
                .flatMap(id -> getCommandStreamIdsByProcedure(id, filter.getControlInputNames(), filter.getValidTimeFilter()));            
        }

        // if command filter is used
        else if (filter.getCommandFilter() != null)
        {
            // get all command stream IDs referenced by commands matching the filter
            idStream = commandStore.select(filter.getCommandFilter(), CommandField.COMMANDSTREAM_ID)
                .map(cmd -> cmd.getCommandStreamID());
        }
        
        // if full-text filter is used, use full-text index as primary
        else if (filter.getFullTextFilter() != null)
        {
            idStream = fullTextIndex.selectKeys(filter.getFullTextFilter());
            fullTextFilterApplied = true;
        }

        // else filter command stream only by input name and valid time
        else
        {
            idStream = getCommandStreamIdsFromAllProcedures(filter.getControlInputNames(), filter.getValidTimeFilter());
        }

        resultStream = idStream
            .map(id -> cmdStreamIndex.getEntry(new CommandStreamKey(id)))
            .filter(Objects::nonNull);
        
        if (filter.getFullTextFilter() != null && !fullTextFilterApplied)
            resultStream = resultStream.filter(e -> filter.testFullText(e.getValue()));
        
        if (filter.getTaskableProperties() != null)
            resultStream = resultStream.filter(e -> filter.testTaskableProperty(e.getValue()));

        if (filter.getValuePredicate() != null)
            resultStream = resultStream.filter(e -> filter.testValuePredicate(e.getValue()));

        // apply limit
        if (filter.getLimit() < Long.MAX_VALUE)
            resultStream = resultStream.limit(filter.getLimit());

        // always wrap with dynamic command stream object
        resultStream = resultStream.map(e -> {
            return new DataUtils.MapEntry<CommandStreamKey, ICommandStreamInfo>(
                e.getKey(),
                new CommandStreamInfoWithTimeRanges(e.getKey().getInternalID(), e.getValue())
            ); 
        });
        
        // apply time filter once validTime is computed correctly
        if (filter.getValidTimeFilter() != null)
            resultStream = resultStream.filter(e -> filter.testValidTime(e.getValue()));
        
        return resultStream;
    }


    @Override
    public ICommandStreamInfo put(CommandStreamKey key, ICommandStreamInfo csInfo)
    {
        DataStoreUtils.checkCommandStreamKey(key);
        try {
            
            DataStoreUtils.checkCommandStreamInfo(procedureStore, csInfo);
            return put(key, csInfo, true);
        }
        catch (DataStoreException e) 
        {
            throw new IllegalArgumentException(e);
        }
    }
    
    
    protected synchronized ICommandStreamInfo put(CommandStreamKey key, ICommandStreamInfo csInfo, boolean replace) throws DataStoreException
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();

            try
            {
                // add to main index
                ICommandStreamInfo oldValue = cmdStreamIndex.put(key, csInfo);
                
                // check if we're allowed to replace existing entry
                boolean isNewEntry = (oldValue == null);
                if (!isNewEntry && !replace)
                    throw new DataStoreException(DataStoreUtils.ERROR_EXISTING_DATASTREAM);
                
                // update proc/output index
                // remove old entry if needed
                if (oldValue != null && replace)
                {
                    MVTimeSeriesProcKey procKey = new MVTimeSeriesProcKey(key.getInternalID(),
                        oldValue.getProcedureID().getInternalID(),
                        oldValue.getControlInputName(),
                        oldValue.getValidTime().begin().getEpochSecond());
                    cmdStreamByProcIndex.remove(procKey);
                }

                // add new entry
                MVTimeSeriesProcKey procKey = new MVTimeSeriesProcKey(key.getInternalID(),
                    csInfo.getProcedureID().getInternalID(),
                    csInfo.getControlInputName(),
                    csInfo.getValidTime().begin().getEpochSecond());
                var oldProcKey = cmdStreamByProcIndex.put(procKey, Boolean.TRUE);
                if (oldProcKey != null && !replace)
                    throw new DataStoreException(DataStoreUtils.ERROR_EXISTING_DATASTREAM);
                
                // update full-text index
                if (isNewEntry)
                    fullTextIndex.add(key.getInternalID(), csInfo);
                else
                    fullTextIndex.update(key.getInternalID(), oldValue, csInfo);
                
                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public synchronized ICommandStreamInfo remove(Object key)
    {
        var dsKey = DataStoreUtils.checkCommandStreamKey(key);

        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();

            try
            {
                // remove all commands
                if (commandStore != null)
                    commandStore.removeAllCommandsAndSeries(dsKey.getInternalID());
                
                // remove from main index
                ICommandStreamInfo oldValue = cmdStreamIndex.remove(dsKey);
                if (oldValue == null)
                    return null;

                // remove entry in secondary index
                cmdStreamByProcIndex.remove(new MVTimeSeriesProcKey(
                    oldValue.getProcedureID().getInternalID(),
                    oldValue.getName(),
                    oldValue.getValidTime().begin()));
                
                // remove from full-text index
                fullTextIndex.remove(dsKey.getInternalID(), oldValue);

                return oldValue;
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public synchronized void clear()
    {
        // synchronize on MVStore to avoid autocommit in the middle of things
        synchronized (mvStore)
        {
            long currentVersion = mvStore.getCurrentVersion();

            try
            {
                commandStore.clear();
                cmdStreamByProcIndex.clear();
                cmdStreamIndex.clear();
            }
            catch (Exception e)
            {
                mvStore.rollbackTo(currentVersion);
                throw e;
            }
        }
    }


    @Override
    public boolean containsKey(Object key)
    {
        var dsKey = DataStoreUtils.checkCommandStreamKey(key);
        return cmdStreamIndex.containsKey(dsKey);
    }


    @Override
    public boolean containsValue(Object val)
    {
        return cmdStreamIndex.containsValue(val);
    }


    @Override
    public Set<Entry<CommandStreamKey, ICommandStreamInfo>> entrySet()
    {
        return cmdStreamIndex.entrySet();
    }


    @Override
    public boolean isEmpty()
    {
        return cmdStreamIndex.isEmpty();
    }


    @Override
    public Set<CommandStreamKey> keySet()
    {
        return cmdStreamIndex.keySet();
    }


    @Override
    public String getDatastoreName()
    {
        return commandStore.getDatastoreName();
    }


    @Override
    public long getNumRecords()
    {
        return cmdStreamIndex.sizeAsLong();
    }


    @Override
    public int size()
    {
        return cmdStreamIndex.size();
    }


    @Override
    public Collection<ICommandStreamInfo> values()
    {
        return cmdStreamIndex.values();
    }


    @Override
    public void commit()
    {
        commandStore.commit();
    }


    @Override
    public void backup(OutputStream is) throws IOException
    {
        throw new UnsupportedOperationException("Call backup on the parent observation store");
    }


    @Override
    public void restore(InputStream os) throws IOException
    {
        throw new UnsupportedOperationException("Call restore on the parent observation store");
    }


    @Override
    public boolean isReadOnly()
    {
        return mvStore.isReadOnly();
    }
    
    
    @Override
    public void linkTo(IProcedureStore procedureStore)
    {
        this.procedureStore = Asserts.checkNotNull(procedureStore, IProcedureStore.class);
    }
}
