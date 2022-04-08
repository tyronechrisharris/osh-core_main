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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.sensorhub.api.command.ICommandStatus;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStatusFilter;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.sensorhub.api.datastore.command.ICommandStatusStore.CommandStatusField;
import org.sensorhub.impl.database.registry.FederatedDatabase.ObsSystemDbFilterInfo;
import org.sensorhub.impl.datastore.MergeSortSpliterator;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of command status store that provides federated read-only
 * access to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 24, 2021
 */
public class FederatedCommandStatusStore extends ReadOnlyDataStore<BigInteger, ICommandStatus, CommandStatusField, CommandStatusFilter> implements ICommandStatusStore
{
    final IDatabaseRegistry registry;
    final FederatedDatabase parentDb;
    final FederatedCommandStore commandStore;
    
    
    FederatedCommandStatusStore(IDatabaseRegistry registry, FederatedDatabase db, FederatedCommandStore cmdStore)
    {
        this.registry = Asserts.checkNotNull(registry, IDatabaseRegistry.class);
        this.parentDb = Asserts.checkNotNull(db, FederatedDatabase.class);
        this.commandStore = cmdStore;
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
        for (var db: parentDb.getAllObsDatabases())
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
    protected ICommandStatus toPublicValue(int databaseID, ICommandStatus status)
    {
        var cmdPublicId = registry.getPublicID(databaseID, status.getCommandID());
        
        // wrap original command status to return correct public IDs
        return new CommandStatusDelegate(status) {
            @Override
            public BigInteger getCommandID()
            {
                return cmdPublicId;
            }
        };
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<BigInteger, ICommandStatus> toPublicEntry(int databaseID, Entry<BigInteger, ICommandStatus> e)
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
        var dbInfo = parentDb.getLocalObsDbInfo(key);
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getCommandStore().containsKey(
                toLocalKey(dbInfo.databaseNum, key));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: parentDb.getAllObsDatabases())
        {
            if (db.getCommandStore().containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public ICommandStatus get(Object obj)
    {
        BigInteger key = ensureCommandKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalObsDbInfo(key);
        if (dbInfo == null)
            return null;
        
        ICommandStatus cmd = dbInfo.db.getCommandStatusStore().get(toLocalKey(dbInfo.databaseNum, key));
        if (cmd == null)
            return null;
        
        return toPublicValue(dbInfo.databaseNum, cmd);
    }
    
    
    protected Map<Integer, ObsSystemDbFilterInfo> getFilterDispatchMap(CommandStatusFilter filter)
    {
        if (filter.getCommandFilter() != null)
        {
            // delegate to command store to get filter dispatch map
            var filterDispatchMap = commandStore.getFilterDispatchMap(filter.getCommandFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = CommandStatusFilter.Builder
                        .from(filter)
                        .withCommands((CommandFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        return null;
    }


    @Override
    public Stream<Entry<BigInteger, ICommandStatus>> selectEntries(CommandStatusFilter filter, Set<CommandStatusField> fields)
    {
        final var cmdStreams = new ArrayList<Stream<Entry<BigInteger, ICommandStatus>>>(100);
        
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            filterDispatchMap.values().stream()
                .forEach(v -> {
                    int dbNum = v.databaseNum;
                    var cmdStream = v.db.getCommandStatusStore().selectEntries((CommandStatusFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                    cmdStreams.add(cmdStream);
                });
        }
        else
        {
            parentDb.getAllObsDatabases().stream()
                .forEach(db -> {
                    int dbNum = db.getDatabaseNum();
                    var cmdStream = db.getCommandStatusStore().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                    cmdStreams.add(cmdStream);
                });
        }
        
        if (cmdStreams.isEmpty())
            return Stream.empty();
        
        // stream and merge commands from all selected command streams and time periods
        var mergeSortIt = new MergeSortSpliterator<Entry<BigInteger, ICommandStatus>>(cmdStreams,
            (e1, e2) -> e1.getValue().getReportTime().compareTo(e2.getValue().getReportTime()));
               
        // stream output of merge sort iterator + apply limit
        return StreamSupport.stream(mergeSortIt, false)
            .limit(filter.getLimit())
            .onClose(() -> mergeSortIt.close());
    }
    
    
    @Override
    public BigInteger add(ICommandStatus cmd)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }

}
