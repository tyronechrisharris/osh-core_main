/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import org.sensorhub.api.datastore.obs.ObsStats;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.resource.ResourceContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.vast.json.JsonInliningWriter;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;


public class ObsStatsBindingJson extends ResourceBindingJson<BigInteger, ObsStats>
{
    JsonInliningWriter writer;
    IdEncoder dsIdEncoder = new IdEncoder(DataStreamHandler.EXTERNAL_ID_SEED);
    IdEncoder foiIdEncoder = new IdEncoder(FoiHandler.EXTERNAL_ID_SEED);

    
    ObsStatsBindingJson(ResourceContext ctx) throws IOException
    {
        super(ctx, new IdEncoder(0));        
        var os = ctx.getOutputStream();
        this.writer = (JsonInliningWriter)getJsonWriter(os, ctx.getPropertyFilter());
    }
    
    
    @Override
    public ObsStats deserialize() throws IOException
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void serialize(BigInteger key, ObsStats stats, boolean showLinks) throws IOException
    {
        writer.beginObject();
        
        var externalDsId = dsIdEncoder.encodeID(stats.getDataStreamID());
        writer.name("datastreamId").value(Long.toString(externalDsId, ResourceBinding.ID_RADIX));
        
        if (stats.getFoiID() != null && stats.getFoiID() != FeatureId.NULL_FEATURE)
        {
            var externalfoiId = foiIdEncoder.encodeID(stats.getFoiID().getInternalID());
            writer.name("foiId").value(Long.toString(externalfoiId, ResourceBinding.ID_RADIX));
        }
        
        if (stats.getPhenomenonTimeRange() != null)
        {
            writer.name("phenomenonTime").beginArray()
                .value(stats.getPhenomenonTimeRange().begin().toString())
                .value(stats.getPhenomenonTimeRange().end().toString())
                .endArray();
        }

        if (stats.getResultTimeRange() != null)
        {
            writer.name("resultTime").beginArray()
                .value(stats.getResultTimeRange().begin().toString())
                .value(stats.getResultTimeRange().end().toString())
                .endArray();
        }
        
        writer.name("obsCount").value(stats.getTotalObsCount());
        
        if (stats.getObsCountsByTime() != null)
        {
            writer.name("histogram").beginArray();
            writer.writeInline(true);
            for (int val: stats.getObsCountsByTime())
                writer.value(val);
            writer.endArray();
            writer.writeInline(false);
        }
        
        writer.endObject();
        writer.flush();
    }


    @Override
    public void startCollection() throws IOException
    {
        startJsonCollection(writer);        
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        endJsonCollection(writer, links);        
    }
}
