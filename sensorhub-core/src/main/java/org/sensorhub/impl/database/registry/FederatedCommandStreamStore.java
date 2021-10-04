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

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.database.IDatabaseRegistry;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore.CommandStreamInfoField;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.system.SystemId;
import org.sensorhub.impl.database.registry.FederatedObsDatabase.LocalFilterInfo;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
import org.sensorhub.impl.datastore.command.CommandStreamInfoWrapper;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of command stream store that provides federated read-only
 * access to several underlying databases.
 * </p>
 *
 * @author Alex Robin
 * @date Mar 24, 2021
 */
public class FederatedCommandStreamStore extends ReadOnlyDataStore<CommandStreamKey, ICommandStreamInfo, CommandStreamInfoField, CommandStreamFilter> implements ICommandStreamStore
{
    final IDatabaseRegistry registry;
    final FederatedObsDatabase parentDb;
    
    
    class CommandStreamInfoWithPublicId extends CommandStreamInfoWrapper
    {
        SystemId publicProcId;        
        
        CommandStreamInfoWithPublicId(SystemId publicProcId, ICommandStreamInfo dsInfo)
        {
            super(dsInfo);
            this.publicProcId = publicProcId;
        }        
        
        @Override
        public SystemId getSystemID()
        {
            return publicProcId;
        }
    }
    
    
    FederatedCommandStreamStore(IDatabaseRegistry registry, FederatedObsDatabase db)
    {
        this.registry = Asserts.checkNotNull(registry, IDatabaseRegistry.class);
        this.parentDb = Asserts.checkNotNull(db, FederatedObsDatabase.class);
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
            count += db.getCommandStore().getCommandStreams().getNumRecords();
        return count;
    }
    
    
    protected CommandStreamKey ensureCommandStreamKey(Object obj)
    {
        Asserts.checkArgument(obj instanceof CommandStreamKey, "key must be a CommandStreamKey");
        return (CommandStreamKey)obj;
    }


    @Override
    public boolean containsKey(Object obj)
    {
        var key = ensureCommandStreamKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(key.getInternalID());
        if (dbInfo == null)
            return false;
        else
            return dbInfo.db.getCommandStore().getCommandStreams().containsKey(new CommandStreamKey(dbInfo.entryID));
    }


    @Override
    public boolean containsValue(Object value)
    {
        for (var db: parentDb.getAllDatabases())
        {
            if (db.getCommandStore().getCommandStreams().containsValue(value))
                return true;
        }
        
        return false;
    }


    @Override
    public ICommandStreamInfo get(Object obj)
    {
        var key = ensureCommandStreamKey(obj);
        
        // use public key to lookup database and local key
        var dbInfo = parentDb.getLocalDbInfo(key.getInternalID());
        if (dbInfo != null)
        {
            ICommandStreamInfo dsInfo = dbInfo.db.getCommandStore().getCommandStreams().get(new CommandStreamKey(dbInfo.entryID));
            if (dsInfo != null)
                return toPublicValue(dbInfo.databaseNum, dsInfo);
        }
        
        return null;
    }
    
    
    /*
     * Convert to public keys on the way out
     */
    protected CommandStreamKey toPublicKey(int databaseID, CommandStreamKey k)
    {
        long publicID = registry.getPublicID(databaseID, k.getInternalID());
        return new CommandStreamKey(publicID);
    }
    
    
    /*
     * Convert to public values on the way out
     */
    protected ICommandStreamInfo toPublicValue(int databaseID, ICommandStreamInfo dsInfo)
    {
        long procPublicID = registry.getPublicID(databaseID, dsInfo.getSystemID().getInternalID());
        SystemId publicId = new SystemId(procPublicID, dsInfo.getSystemID().getUniqueID());
        return new CommandStreamInfoWithPublicId(publicId, dsInfo);
    }
    
    
    /*
     * Convert to public entries on the way out
     */
    protected Entry<CommandStreamKey, ICommandStreamInfo> toPublicEntry(int databaseID, Entry<CommandStreamKey, ICommandStreamInfo> e)
    {
        return new AbstractMap.SimpleEntry<>(
            toPublicKey(databaseID, e.getKey()),
            toPublicValue(databaseID, e.getValue()));
    }
    
    
    /*
     * Get dispatch map according to internal IDs used in filter
     */
    protected Map<Integer, LocalFilterInfo> getFilterDispatchMap(CommandStreamFilter filter)
    {
        if (filter.getInternalIDs() != null)
        {
            var filterDispatchMap = parentDb.getFilterDispatchMap(filter.getInternalIDs());
            for (var filterInfo: filterDispatchMap.values())
            {
                filterInfo.filter = CommandStreamFilter.Builder
                    .from(filter)
                    .withInternalIDs(filterInfo.internalIds)
                    .build();
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getSystemFilter() != null)
        {
            // delegate to system store to get filter dispatch map
            var filterDispatchMap = parentDb.systemStore.getFilterDispatchMap(filter.getSystemFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = CommandStreamFilter.Builder
                        .from(filter)
                        .withSystems((SystemFilter)filterInfo.filter)
                        .build();
                }
            }
            
            return filterDispatchMap;
        }
        
        else if (filter.getCommandFilter() != null)
        {
            // delegate to command store handle command filter dispatch map
            var filterDispatchMap = parentDb.commandStore.getFilterDispatchMap(filter.getCommandFilter());
            if (filterDispatchMap != null)
            {
                for (var filterInfo: filterDispatchMap.values())
                {
                    filterInfo.filter = CommandStreamFilter.Builder
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
    public Stream<Entry<CommandStreamKey, ICommandStreamInfo>> selectEntries(CommandStreamFilter filter, Set<CommandStreamInfoField> fields)
    {
        // if any kind of internal IDs are used, we need to dispatch the correct filter
        // to the corresponding DB so we create this map
        var filterDispatchMap = getFilterDispatchMap(filter);
        
        if (filterDispatchMap != null)
        {
            return filterDispatchMap.values().stream()
                .flatMap(v -> {
                    int dbNum = v.databaseNum;
                    return v.db.getCommandStore().getCommandStreams().selectEntries((CommandStreamFilter)v.filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                })
                .limit(filter.getLimit());
        }
        
        // otherwise scan all DBs
        else
        {
            return parentDb.getAllDatabases().stream()
                .flatMap(db -> {
                    int dbNum = db.getDatabaseNum();
                    return db.getCommandStore().getCommandStreams().selectEntries(filter, fields)
                        .map(e -> toPublicEntry(dbNum, e));
                })
                .limit(filter.getLimit());
        }
    }


    @Override
    public CommandStreamKey add(ICommandStreamInfo dsInfo)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }


    @Override
    public void linkTo(ISystemDescStore systemStore)
    {
        throw new UnsupportedOperationException();        
    }

}
