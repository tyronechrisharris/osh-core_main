/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.obs;

import org.sensorhub.api.feature.IFeatureStore;
import org.sensorhub.api.feature.IFeatureStoreBase;
import org.sensorhub.api.feature.IFeatureStoreBase.FeatureField;
import org.sensorhub.api.obs.IFoiStore.FoiField;
import org.vast.ogc.gml.IGeoFeature;


/**
 * <p>
 * Generic interface for all feature of interest stores
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2018
 */
public interface IFoiStore extends IFeatureStoreBase<IGeoFeature, FoiField, FoiFilter>
{
    
    public static class FoiField extends FeatureField
    {
        public static final FoiField SAMPLING_FEATURE = new FoiField("samplingFeature");
        
        public FoiField(String name)
        {
            super(name);
        }
    }
    
    
    @Override
    public default FoiFilter.Builder filterBuilder()
    {
        return new FoiFilter.Builder();
    }
    
    
    /**
     * Link this store to an observation store to enable JOIN queries
     * @param obsStore
     */
    public void linkTo(IObsStore obsStore);
    
    
    /**
     * Link this store to a sampled feature store to enable JOIN queries
     * @param featureStore
     */
    public void linkTo(IFeatureStore featureStore);
}
