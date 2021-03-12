/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.view;

import java.math.BigInteger;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.command.ICommandDataWithAck;
import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStats;
import org.sensorhub.api.datastore.command.CommandStatsQuery;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.command.ICommandStore.CommandField;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;


/**
 * <p>
 * Filtered view implemented as a wrapper for an instance of ICommandStore
 * </p>
 *
 * @author Alex Robin
 * @date Mar 12, 2021
 */
public class CommandStoreView extends ReadOnlyDataStore<BigInteger, ICommandDataWithAck, CommandField, CommandFilter> implements ICommandStore
{
    ICommandStore delegate;
    CommandStreamStoreView commandStreamStoreView;
    CommandFilter viewFilter;
    
    
    public CommandStoreView(ICommandStore delegate, CommandFilter viewFilter)
    {
        this.delegate = delegate;
        this.commandStreamStoreView = new CommandStreamStoreView(delegate.getCommandStreams(), viewFilter.getCommandStreamFilter());
        this.viewFilter = viewFilter;
    }


    @Override
    public Stream<Entry<BigInteger, ICommandDataWithAck>> selectEntries(CommandFilter filter, Set<CommandField> fields)
    {
        try
        {
            if (viewFilter != null)
                filter = viewFilter.intersect(filter);
            return delegate.selectEntries(filter, fields);
        }
        catch (EmptyFilterIntersection e)
        {
            return Stream.empty();
        }
    }
    
    
    @Override
    public long countMatchingEntries(CommandFilter filter)
    {
        try
        {
            if (viewFilter != null)
                filter = viewFilter.intersect(filter);
            return delegate.countMatchingEntries(filter);
        }
        catch (EmptyFilterIntersection e)
        {
            return 0L;
        } 
    }


    @Override
    public ICommandDataWithAck get(Object key)
    {
        return delegate.get(key);
    }


    @Override
    public Stream<CommandStats> getStatistics(CommandStatsQuery query)
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public String getDatastoreName()
    {
        return delegate.getDatastoreName();
    }


    @Override
    public ICommandStreamStore getCommandStreams()
    {
        return commandStreamStoreView;
    }


    @Override
    public BigInteger add(ICommandDataWithAck obs)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }
}
