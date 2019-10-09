/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.sensorhub.api.datastore.IObsStore;
import org.sensorhub.api.datastore.ObsData;
import org.sensorhub.api.datastore.ObsFilter;
import org.sensorhub.api.datastore.ObsKey;
import org.sensorhub.api.datastore.ObsStats;
import org.sensorhub.api.datastore.ObsStatsQuery;
import net.opengis.swe.v20.DataBlock;

/**
 * <p>
 * Implementation of observation store that partitions observations into
 * multiple MVObsStore, each attached to a separate MVStore instance.
 * Observations from all data streams sharing the same procedure or procedure
 * group are stored in the same partition.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 19, 2019
 */
public class MVMultiObsStoreImpl implements IObsStore
{
    MVDataStoreInfo dataStoreInfo;
    MVDataStreamStoreImpl dataStreamStore;
    Map<Long, MVObsStoreImpl> obsStores;
    
    
    @Override
    public String getDatastoreName()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ZoneOffset getTimeZone()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long getNumRecords()
    {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public Stream<ObsData> select(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Stream<ObsKey> selectKeys(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Stream<Entry<ObsKey, ObsData>> selectEntries(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Stream<ObsKey> removeEntries(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public long countMatchingEntries(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public void commit()
    {
        // TODO Auto-generated method stub
    }


    @Override
    public void backup(OutputStream is) throws IOException
    {
        // TODO Auto-generated method stub

    }


    @Override
    public void restore(InputStream os) throws IOException
    {
        // TODO Auto-generated method stub

    }


    @Override
    public boolean isReadSupported()
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public boolean isWriteSupported()
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public void clear()
    {
        // TODO Auto-generated method stub

    }


    @Override
    public boolean containsKey(Object arg0)
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public boolean containsValue(Object arg0)
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public Set<Entry<ObsKey, ObsData>> entrySet()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ObsData get(Object arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public boolean isEmpty()
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public Set<ObsKey> keySet()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public ObsData put(ObsKey arg0, ObsData arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void putAll(Map<? extends ObsKey, ? extends ObsData> arg0)
    {
        // TODO Auto-generated method stub

    }


    @Override
    public ObsData remove(Object arg0)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public int size()
    {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public Collection<ObsData> values()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Stream<DataBlock> selectResults(ObsFilter query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query)
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public IDataStreamStore getDataStreams()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
