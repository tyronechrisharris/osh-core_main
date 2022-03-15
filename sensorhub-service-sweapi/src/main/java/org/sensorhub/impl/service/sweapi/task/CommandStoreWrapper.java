/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.task;

import java.math.BigInteger;
import java.util.stream.Stream;
import org.sensorhub.api.command.ICommandData;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStats;
import org.sensorhub.api.datastore.command.CommandStatsQuery;
import org.sensorhub.api.datastore.command.ICommandStatusStore;
import org.sensorhub.api.datastore.command.ICommandStore;
import org.sensorhub.api.datastore.command.ICommandStore.CommandField;
import org.sensorhub.api.datastore.command.ICommandStreamStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.impl.service.sweapi.AbstractDataStoreWrapper;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.vast.util.Asserts;


public class CommandStoreWrapper extends AbstractDataStoreWrapper<BigInteger, ICommandData, CommandField, CommandFilter, ICommandStore> implements ICommandStore
{
    final IdConverter idConverter;
    
    
    public CommandStoreWrapper(ICommandStore readStore, ICommandStore writeStore, IdConverter idConverter)
    {
        super(readStore, writeStore);
        this.idConverter = Asserts.checkNotNull(idConverter, IdConverter.class);
    }


    @Override
    public CommandFilter.Builder filterBuilder()
    {
        return new CommandFilter.Builder();
    }


    @Override
    public BigInteger add(ICommandData cmd)
    {
        return getWriteStore().add(cmd);
    }


    @Override
    public Stream<CommandStats> getStatistics(CommandStatsQuery query)
    {
        return getReadStore().getStatistics(query);
    }


    @Override
    public ICommandStreamStore getCommandStreams()
    {
        return getReadStore().getCommandStreams();
    }


    @Override
    public ICommandStatusStore getStatusReports()
    {
        return getReadStore().getStatusReports();
    }


    @Override
    public void linkTo(IFoiStore foiStore)
    {
        throw new UnsupportedOperationException();
    }

}
