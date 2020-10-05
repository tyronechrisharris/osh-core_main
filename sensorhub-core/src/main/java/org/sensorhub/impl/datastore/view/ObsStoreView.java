/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.view;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.obs.IDataStreamStore;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.obs.ObsFilter;
import org.sensorhub.api.obs.ObsStats;
import org.sensorhub.api.obs.ObsStatsQuery;


public class ObsStoreView implements IObsStore
{
    IObsStore delegate;
    DataStreamStoreView dataStreamStoreView;
    
    
    public ObsStoreView(IObsStore delegate)
    {
        this.delegate = delegate;
        this.dataStreamStoreView = new DataStreamStoreView(delegate.getDataStreams());
    }
    
    
    @Override
    public String getDatastoreName()
    {
        return delegate.getDatastoreName();
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        return delegate.getTimeZone();
    }


    @Override
    public IDataStreamStore getDataStreams()
    {
        return delegate.getDataStreams();
    }


    @Override
    public long getNumRecords()
    {
        return delegate.getNumRecords();
    }


    @Override
    public Stream<Entry<BigInteger, IObsData>> selectEntries(ObsFilter query, Set<ObsField> fields)
    {
        return delegate.selectEntries(query, fields);
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        return delegate.getStatistics(query);
    }


    @Override
    public int size()
    {
        return delegate.size();
    }


    @Override
    public boolean isEmpty()
    {
        return delegate.isEmpty();
    }


    @Override
    public boolean containsKey(Object key)
    {
        return delegate.containsKey(key);
    }


    @Override
    public boolean containsValue(Object value)
    {
        return delegate.containsValue(value);
    }


    @Override
    public IObsData get(Object key)
    {
        return delegate.get(key);
    }


    @Override
    public Set<BigInteger> keySet()
    {
        return delegate.keySet();
    }


    @Override
    public Collection<IObsData> values()
    {
        return delegate.values();
    }


    @Override
    public Set<Entry<BigInteger, IObsData>> entrySet()
    {
        return delegate.entrySet();
    }


    @Override
    public boolean isReadSupported()
    {
        return delegate.isReadSupported();
    }


    @Override
    public boolean isWriteSupported()
    {
        return false;
    }


    @Override
    public void commit()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void backup(OutputStream is) throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void restore(InputStream os) throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public BigInteger add(IObsData obs)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public IObsData put(BigInteger key, IObsData value)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public IObsData remove(Object key)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }
}
