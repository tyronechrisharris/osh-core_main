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

import java.io.IOException;
import java.util.Map;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.impl.service.sweapi.ResourceContext.ResourceRef;
import org.vast.ogc.gml.IGeoFeature;


public class FoiHandler extends AbstractFeatureHandler<IGeoFeature, FoiFilter, FoiFilter.Builder, IFoiStore>
{
    public static final String NAME = "fois";
    
    
    public FoiHandler(IFoiStore dataStore)
    {
        super(dataStore, new FeatureResourceType());
    }
    
    
    @Override
    public boolean doPost(ResourceContext ctx) throws IOException
    {
        if (ctx.isEmpty() && !(ctx.getParentRef().type instanceof FeatureCollectionResourceType))
            return ctx.sendError(405, "Features can only be created within Feature Collections");
        
        return super.doPost(ctx);
    }


    @Override
    protected void buildFilter(ResourceRef parent, Map<String, String[]> queryParams, FoiFilter.Builder builder) throws InvalidRequestException
    {
        super.buildFilter(parent, queryParams, builder);
        
        if (parent.internalID > 0)
        {
            builder.withObservations()
                .withProcedures(parent.internalID)
                .done();
        }
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
