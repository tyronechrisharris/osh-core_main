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
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.sensorhub.api.feature.FeatureKey;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.procedure.IProcedureStore;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureFilter;
import org.vast.util.Bbox;


public class ProcedureStoreView implements IProcedureStore
{
    IProcedureStore delegate;
    ProcedureFilter viewFilter;
    

    public ProcedureStoreView(IProcedureStore delegate, ProcedureFilter viewFilter)
    {
        this.delegate = delegate;
        this.viewFilter = viewFilter;
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
    public long getNumRecords()
    {
        return delegate.getNumRecords();
    }


    @Override
    public long getNumFeatures()
    {
        return delegate.getNumFeatures();
    }


    @Override
    public Bbox getFeaturesBbox()
    {
        return delegate.getFeaturesBbox();
    }


    @Override
    public Stream<Entry<FeatureKey, IProcedureWithDesc>> selectEntries(ProcedureFilter filter, Set<ProcedureField> fields)
    {
        try
        {
            return delegate.selectEntries(viewFilter.and(filter), fields);
        }
        catch (EmptyFilterIntersection e)
        {
            return Stream.empty();
        }
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
    public IProcedureWithDesc get(Object key)
    {
        return delegate.get(key);
    }


    @Override
    public Set<FeatureKey> keySet()
    {
        return delegate.keySet();
    }


    @Override
    public Collection<IProcedureWithDesc> values()
    {
        return delegate.values();
    }


    @Override
    public Set<Entry<FeatureKey, IProcedureWithDesc>> entrySet()
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
    public FeatureKey add(IProcedureWithDesc proc)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public FeatureKey add(long parentId, IProcedureWithDesc value)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public FeatureKey addVersion(IProcedureWithDesc proc)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public IProcedureWithDesc put(FeatureKey key, IProcedureWithDesc value)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public IProcedureWithDesc remove(Object key)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
        throw new UnsupportedOperationException();
    }
}