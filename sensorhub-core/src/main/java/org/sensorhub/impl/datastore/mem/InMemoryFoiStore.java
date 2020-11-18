/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.mem;

import java.util.stream.Stream;
import org.sensorhub.api.datastore.IdProvider;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.feature.IFoiStore.FoiField;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.util.Asserts;


/**
 * <p>
 * In-memory implementation of FOI store backed by a {@link java.util.NavigableMap}.
 * This implementation is only used to store the latest feature state and thus
 * doesn't support versioning/history of FOI descriptions.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 28, 2019
 */
public class InMemoryFoiStore extends InMemoryBaseFeatureStore<IGeoFeature, FoiField, FoiFilter> implements IFoiStore
{
    IObsStore obsStore;
    
    
    public InMemoryFoiStore()
    {
        this.idProvider = new HashCodeFeatureIdProvider(806335237);
    }
    
    
    public InMemoryFoiStore(IdProvider<? super IGeoFeature> idProvider)
    {
        this.idProvider = Asserts.checkNotNull(idProvider, IdProvider.class);            
    }
    
    
    @Override
    protected Stream<Entry<FeatureKey, IGeoFeature>> getIndexedStream(FoiFilter filter)
    {
        if (filter.getObservationFilter() != null)
        {
            var idStream = obsStore.selectObservedFois(filter.getObservationFilter());
            return entryStream(idStream);
        }
        
        else if (filter.getSampledFeatureFilter() != null)
        {
            // TODO
        }
        
        return super.getIndexedStream(filter);
    }
    

    @Override
    public void linkTo(IObsStore obsStore)
    {
        this.obsStore = Asserts.checkNotNull(obsStore, IObsStore.class);
    }


    @Override
    public void linkTo(IFeatureStore featureStore)
    {
        throw new UnsupportedOperationException();
    }
}
