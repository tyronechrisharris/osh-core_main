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

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.sensorhub.api.datastore.feature.FeatureFilterBase.FeatureFilterBaseBuilder;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext.ResourceRef;
import org.vast.ogc.gml.IFeature;


public abstract class AbstractFeatureHandler<
    V extends IFeature,
    F extends FeatureFilterBase<? super V>,
    B extends FeatureFilterBaseBuilder<B,? super V,F>,
    S extends IFeatureStoreBase<V,?,F>> extends ResourceHandler<FeatureKey, V, F, B, S>
{
    
    protected AbstractFeatureHandler(S dataStore, ResourceType<FeatureKey, V> resourceType)
    {
        super(dataStore, resourceType);
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEmpty() && !(ctx.getParentRef().type instanceof FeatureCollectionResourceType))
            return ctx.sendError(405, "Features can only be created within Feature Collections");
        
        return super.doPost(ctx);
    }


    @Override
    protected void buildFilter(final ResourceRef parent, final Map<String, String[]> queryParams, final B builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        String[] paramValues;
        
        // uid param
        paramValues = queryParams.get("uid");
        if (paramValues != null)
        {
            var featureUIDs = parseMultiValuesArg(paramValues);
            builder.withUniqueIDs(featureUIDs);
        }
        
        // validTime param
        paramValues = queryParams.get("validTime");
        if (paramValues != null)
        {
            var timeExtent = parseTimeStampArg(paramValues);
            builder.withValidTime(new TemporalFilter.Builder()
                .fromTimeExtent(timeExtent)
                .build());
        }
        else
            builder.withCurrentVersion();
        
        // use opensearch bbox param to filter spatially
        paramValues = queryParams.get("bbox");
        if (paramValues != null)
        {
            var bbox = parseBboxArg(paramValues);
            builder.withLocationWithin(bbox);
        }
        
        // geom param
        paramValues = queryParams.get("geom");
        if (paramValues != null)
        {
            var geom = parseGeomArg(paramValues);
            builder.withLocationIntersecting(geom);
        }
    }


    @Override
    protected FeatureKey getKey(long internalID)
    {
        var fk = dataStore.getCurrentVersionKey(internalID);
        if (fk != null)
            return fk;
        else // return an unknown key so the caller sends 404
            return new FeatureKey(Long.MAX_VALUE);
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
