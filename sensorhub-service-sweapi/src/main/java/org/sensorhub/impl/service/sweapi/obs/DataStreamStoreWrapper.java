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

import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IDataStreamStore.DataStreamInfoField;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.api.system.SystemId;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.resource.AbstractResourceStoreWrapper;
import org.vast.util.Asserts;


public class DataStreamStoreWrapper extends AbstractResourceStoreWrapper<DataStreamKey, IDataStreamInfo, DataStreamInfoField, DataStreamFilter, IDataStreamStore> implements IDataStreamStore
{
    final IdConverter idConverter;
    
    
    public DataStreamStoreWrapper(IDataStreamStore readStore, IDataStreamStore writeStore, IdConverter idConverter)
    {
        super(readStore, writeStore);
        this.idConverter = Asserts.checkNotNull(idConverter, IdConverter.class);
    }


    @Override
    public DataStreamFilter.Builder filterBuilder()
    {
        return (DataStreamFilter.Builder)super.filterBuilder();
    }
    
    
    @Override
    public DataStreamKey add(IDataStreamInfo value) throws DataStoreException
    {
        var sysID = idConverter.toInternalID(value.getSystemID().getInternalID()); 
        var sysUID = value.getSystemID().getUniqueID();
                
        value = DataStreamInfo.Builder.from(value)
            .withSystem(new SystemId(sysID, sysUID))
            .build();
        
        return toPublicKey(getWriteStore().add(value));
    }


    @Override
    public void linkTo(ISystemDescStore systemStore)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    protected DataStreamKey toInternalKey(DataStreamKey publicKey)
    {
        return new DataStreamKey(
            idConverter.toInternalID(publicKey.getInternalID()));
    }


    @Override
    protected DataStreamKey toPublicKey(DataStreamKey internalKey)
    {
        return new DataStreamKey(
            idConverter.toPublicID(internalKey.getInternalID()));
    }

}
