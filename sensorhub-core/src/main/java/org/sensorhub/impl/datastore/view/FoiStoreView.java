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

import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.feature.IFoiStore.FoiField;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.vast.ogc.gml.IGeoFeature;


/**
 * <p>
 * Filtered view implemented as a wrapper for an instance of IFoiStore
 * </p>
 *
 * @author Alex Robin
 * @date Nov 3, 2020
 */
public class FoiStoreView extends FeatureStoreViewBase<IGeoFeature, FoiField, FoiFilter, IFoiStore> implements IFoiStore
{        
    
    public FoiStoreView(IFoiStore delegate, FoiFilter viewFilter)
    {
        super(delegate, viewFilter);
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void linkTo(IFeatureStore featureStore)
    {
        throw new UnsupportedOperationException();        
    }
}
