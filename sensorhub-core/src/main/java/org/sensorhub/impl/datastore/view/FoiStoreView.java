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

import org.sensorhub.api.obs.FoiFilter;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.obs.IFoiStore.FoiField;
import org.vast.ogc.gml.IGeoFeature;


public class FoiStoreView extends FeatureStoreViewBase<IGeoFeature, FoiField, FoiFilter, IFoiStore> implements IFoiStore
{        
    
    public FoiStoreView(IFoiStore delegate, FoiFilter viewFilter)
    {
        super(delegate, viewFilter);
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }
}
