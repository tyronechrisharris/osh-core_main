/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import java.util.Map;
import org.sensorhub.api.datastore.feature.FeatureFilter;
import org.sensorhub.api.datastore.feature.IFeatureStore;
import org.sensorhub.api.datastore.feature.FeatureFilter.Builder;
import org.sensorhub.impl.service.sweapi.ResourceContext.ResourceRef;
import org.vast.ogc.gml.IGeoFeature;


public class FeatureHandler extends AbstractFeatureHandler<IGeoFeature, FeatureFilter, FeatureFilter.Builder, IFeatureStore>
{
    public static final String NAME = "features";
    
    
    public FeatureHandler(IFeatureStore dataStore)
    {
        super(dataStore, new FeatureResourceType());
        addSubResource(new FeatureHistoryHandler(dataStore));
    }


    @Override
    protected void buildFilter(ResourceRef parent, Map<String, String[]> queryParams, Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
    }


    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    protected void validate(IGeoFeature resource)
    {
        // TODO Auto-generated method stub
        
    }
}
