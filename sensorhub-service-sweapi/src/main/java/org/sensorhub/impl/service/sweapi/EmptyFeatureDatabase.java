/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;
import org.sensorhub.api.database.IFeatureDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.impl.datastore.ReadOnlyDataStore;
import org.vast.ogc.gml.IFeature;
import org.vast.util.Bbox;

public class EmptyFeatureDatabase implements IFeatureDatabase
{
    static class EmptyFeatureStore extends ReadOnlyDataStore<FeatureKey, IFeature, FeatureField, FeatureFilter> implements IFeatureStore
    {
        
        public EmptyFeatureStore()
        {
        }


        @Override
        public String getDatastoreName()
        {
            return "Empty Feature Store";
        }


        @Override
        public Stream<Entry<FeatureKey, IFeature>> selectEntries(FeatureFilter query, Set<FeatureField> fields)
        {
            return Stream.empty();
        }


        @Override
        public IFeature get(Object key)
        {
            return null;
        }


        @Override
        public FeatureKey add(IFeature f) throws DataStoreException
        {
            throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
        }


        @Override
        public FeatureKey add(long parentID, IFeature value) throws DataStoreException
        {
            throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
        }


        @Override
        public Long getParent(long internalID)
        {
            return null;
        }


        @Override
        public Bbox getFeaturesBbox()
        {
            return new Bbox();
        }
    }
    
    
    @Override
    public Integer getDatabaseNum()
    {
        return 1000;
    }


    @Override
    public <T> T executeTransaction(Callable<T> transaction) throws Exception
    {
        return null;
    }


    @Override
    public void commit()
    {
    }


    @Override
    public boolean isOpen()
    {
        return true;
    }


    @Override
    public boolean isReadOnly()
    {
        return true;
    }


    @Override
    public IFeatureStore getFeatureStore()
    {
        return new EmptyFeatureStore();
    }

}
