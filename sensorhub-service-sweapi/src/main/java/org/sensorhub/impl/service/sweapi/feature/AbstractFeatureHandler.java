/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.feature;

import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.FeatureFilterBase.FeatureFilterBaseBuilder;
import org.sensorhub.impl.service.sweapi.IdConverter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.SWEApiSecurity.ResourcePermissions;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.ogc.gml.IFeature;


public abstract class AbstractFeatureHandler<
    V extends IFeature,
    F extends FeatureFilterBase<? super V>,
    B extends FeatureFilterBaseBuilder<B,? super V,F>,
    S extends IFeatureStoreBase<V,?,F>> extends ResourceHandler<FeatureKey, V, F, B, S>
{
    IdConverter idConverter;
    
    
    protected AbstractFeatureHandler(S dataStore, IdEncoder idEncoder, ResourcePermissions permissions)
    {
        super(dataStore, idEncoder, permissions);
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final B builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        // uid param
        var featureUIDs = parseMultiValuesArg("uid", queryParams);
        if (featureUIDs != null && !featureUIDs.isEmpty())
            builder.withUniqueIDs(featureUIDs);
                
        // validTime param
        var timeExtent = parseTimeStampArg("validTime", queryParams);
        if (timeExtent != null)
        {
            builder.withValidTime(new TemporalFilter.Builder()
                .fromTimeExtent(timeExtent)
                .build());
        }
        else
            builder.withCurrentVersion();
        
        // use opensearch bbox param to filter spatially
        var bbox = parseBboxArg("bbox", queryParams);
        if (bbox != null)
            builder.withLocationWithin(bbox);
        
        // geom param
        var geom = parseGeomArg("geom", queryParams);
        if (geom != null)
            builder.withLocationIntersecting(geom);
    }


    @Override
    protected FeatureKey getKey(long publicID)
    {
        return dataStore.getCurrentVersionKey(publicID);
    }
    
    
    protected FeatureKey getKey(long internalID, long version)
    {
        if (version == 0)
            return getKey(internalID);
        
        return dataStore.selectKeys(dataStore.filterBuilder()
                .withInternalIDs(internalID)
                .validAtTime(Instant.ofEpochSecond(version))
                .build())
            .findFirst()
            .orElse(null);
    }
}
