/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.math.BigInteger;
import java.util.stream.Stream;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStats;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.datastore.obs.IObsStore.ObsField;
import org.sensorhub.impl.service.sweapi.AbstractDataStoreWrapper;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.vast.util.Asserts;


public class ObsStoreWrapper extends AbstractDataStoreWrapper<BigInteger, IObsData, ObsField, ObsFilter, IObsStore> implements IObsStore
{
    final IdConverter idConverter;
    
    
    public ObsStoreWrapper(IObsStore readStore, IObsStore writeStore, IdConverter idConverter)
    {
        super(readStore, writeStore);
        this.idConverter = Asserts.checkNotNull(idConverter, IdConverter.class);
    }


    @Override
    public ObsFilter.Builder filterBuilder()
    {
        return new ObsFilter.Builder();
    }


    @Override
    public BigInteger add(IObsData obs)
    {
        return getWriteStore().add(obs);
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        return getReadStore().getStatistics(query);
    }


    @Override
    public void linkTo(IFoiStore foiStore)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public IDataStreamStore getDataStreams()
    {
        return getReadStore().getDataStreams();
    }

}
