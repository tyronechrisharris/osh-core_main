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

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStats;
import org.sensorhub.api.datastore.command.CommandStatsQuery;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStore.CommandField;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.impl.database.registry.FederatedObsDatabase.LocalFilterInfo;
import org.sensorhub.impl.datastore.MergeSortSpliterator;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of command store that provides federated read-only access
 * to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 24, 2021
 */
public class FederatedCommandStore extends ReadOnlyDataStore<BigInteger, ICommandData, CommandField, CommandFilter> implements ICommandStore
{
    final IDatabaseRegistry registry;
    final FederatedObsDatabase parentDb;
    final FederatedCommandStreamStore commandStreamStore;
    final FederatedCommandStatusStore commandStatusStore;
    
    
    FederatedCommandStore(IDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = Asserts.checkNotNull(registry, IDatabaseRegistry.class);
        this.parentDb = Asserts.checkNotNull(db, FederatedObsDatabase.class);
        this.commandStreamStore = new FederatedCommandStreamStore(registry, db);
        this.commandStatusStore = new FederatedCommandStatusStore(registry, db, this);
    }


    @Override
    public String getDatastoreName()
    {
        return getClass().getSimpleName();
    }


    @Override
    public long getNumRecords()
    {
        long count = 0;
        for (var db: parentDb.getAllDatabases())
            count += db.getCommandStore().getNumRecords();
        return count;
    }
    
    
    protected BigInteger ensureCommandKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof BigInteger, "Key must be a BigInteger");
        return (BigInteger)obj;
    }
    
    
    /*
     * Convert to local keys on the way in
     */
    protected BigInteger toLocalKey(int databaseID, BigInteger key)
    {
        return registry.getLocalID(databaseID, key);
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected BigInteger toPublicKey(int databaseID, BigInteger k)
    {
        return registry.getPublicID(databaseID, k);
    }
    
    
    /*
     * Convert to public values on the way out
     */
    protected ICommandData toPublicValue(int databaseID, ICommandData cmd)
    {
        long dsPublicId = registry.getPublicID(databaseID, cmd.getCommandStreamID());
        
        // wrap original command to return correct public IDs
        return new CommandDelegate(cmd) {
            @Override
            public long getCommandStreamID()
            {
                return dsPublicId;
            }
        };
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<BigInteger, ICommandData> toPublicEntry(int databaseID, Entry<BigInteger, ICommandData> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            toPublicValue(databaseID, e.getValue()));
    }


    @Override
    public boolean containsKey(Object obj)
    {
        BigInteger key = ensureCommandKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(key);
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getCommandStore().containsKey(
                toLocalKey(dbInfo.databaseNum, key));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: parentDb.getAllDatabases())
        {
            if (db.getCommandStore().containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public ICommandData get(Object obj)
    {
        BigInteger key = ensureCommandKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(key);
        if (dbInfo == null)
            return null;
        
        ICommandData cmd = dbInfo.db.getCommandStore().get(toLocalKey(dbInfo.databaseNum, key));
        if (cmd == null)
            return null;
        
        return toPublicValue(dbInfo.databaseNum, cmd);
    }
    
    
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(CommandFilter filter)
    {
        Map<Integer, LocalFilterInfo> commandStreamFilterDispatchMap = null;
        Map<Integer, LocalFilterInfo> commandFilterDispatchMap = new TreeMap<>();
        
        // use internal IDs if present
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = parentDb.getFilterDispatchMapBigInt(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = CommandFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.bigInternalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        // otherwise get dispatch map for command streams
        if (filter.getCommandStreamFilter() != null)
            commandStreamFilterDispatchMap = commandStreamStore.getFilterDispatchMap(filter.getCommandStreamFilter());
        
        if (commandStreamFilterDispatchMap != null)
        {
            for (var entry: commandStreamFilterDispatchMap.entrySet())
            {
                var commandStreamFilterInfo = entry.getValue();
                
                // only process DBs not already processed in first loop above
                if (!commandFilterDispatchMap.containsKey(entry.getKey()))
                {
                    var filterInfo = new LocalFilterInfo();
                    filterInfo.databaseNum = commandStreamFilterInfo.databaseNum;
                    filterInfo.db = commandStreamFilterInfo.db;
                    filterInfo.filter = CommandFilter.Builder.from(filter)
                        .withCommandStreams((CommandStreamFilter)commandStreamFilterInfo.filter)
                        .build();
                    commandFilterDispatchMap.put(entry.getKey(), filterInfo);
                }
            }
        }
        
        if (!commandFilterDispatchMap.isEmpty())
            return commandFilterDispatchMap;
        else
            return null;
    }


    @Override
    public Stream<Entry<BigInteger, ICommandData>> selectEntries(CommandFilter filter, Set<CommandField> fields)
    {
        final var cmdStreams = new ArrayList<Stream<Entry<BigInteger, ICommandData>>>(100);
        
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            filterDispatchMap.values().stream()
                .forEach(v -> {
                    int dbNum = v.databaseNum;
                    var cmdStream = v.db.getCommandStore().selectEntries((CommandFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                    cmdStreams.add(cmdStream);
                });
        }
        else
        {
            parentDb.getAllDatabases().stream()
                .forEach(db -> {
                    int dbNum = db.getDatabaseNum();
                    var cmdStream = db.getCommandStore().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                    cmdStreams.add(cmdStream);
                });
        }
        
        
        // stream and merge commands from all selected command streams and time periods
        var mergeSortIt = new MergeSortSpliterator<Entry<BigInteger, ICommandData>>(cmdStreams,
            (e1, e2) -> e1.getValue().getIssueTime().compareTo(e2.getValue().getIssueTime()));
               
        // stream output of merge sort iterator + apply limit
        return StreamSupport.stream(mergeSortIt, false)
            .limit(filter.getLimit())
            .onClose(() -> mergeSortIt.close());
    }
    
    
    /*
     * Convert to public values on the way out
     */
    protected CommandStats toPublicStats(int databaseID, CommandStats stats)
    {
        long dsPublicID = registry.getPublicID(databaseID, stats.getCommandStreamID());
            
        // create stats object with public IDs
        return CommandStats.Builder.from(stats)
            .withCommandStreamID(dsPublicID)
            .build();
    }


    @Override
    public Stream<CommandStats> getStatistics(CommandStatsQuery query)
    {
        var filter = query.getCommandFilter();
        
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbNum = v.databaseNum;
                    var dbQuery = CommandStatsQuery.Builder.from(query)
                        .selectCommands((CommandFilter)v.filter)
                        .build();
                    return v.db.getCommandStore().getStatistics(dbQuery)
                        .map(stats -> toPublicStats(dbNum, stats));
                })
                .limit(filter.getLimit());
        }
        else
        {
            return parentDb.getAllDatabases().stream()
                .flatMap(db -> {
                    int dbNum = db.getDatabaseNum();
                    return db.getCommandStore().getStatistics(query)
                        .map(stats -> toPublicStats(dbNum, stats));
                })
                .limit(filter.getLimit());
        }
    }
    

    @Override
    public ICommandStreamStore getCommandStreams()
    {
        return commandStreamStore;
    }


    @Override
    public ICommandStatusStore getStatusReports()
    {
        return commandStatusStore;
    }
    
    
    @Override
    public BigInteger add(ICommandData cmd)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public void linkTo(IFoiStore foiStore)
    {
        throw new UnsupportedOperationException();
    }

}
