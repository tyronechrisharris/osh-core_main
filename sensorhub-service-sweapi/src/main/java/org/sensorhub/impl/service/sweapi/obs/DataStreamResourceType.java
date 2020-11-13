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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.impl.service.sweapi.IdUtils;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.stream.JsonWriter;


public class DataStreamResourceType extends ResourceType<DataStreamKey, IDataStreamInfo>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    
    
    DataStreamResourceType()
    {
        super(new IdUtils(EXTERNAL_ID_SEED));
    }


    @Override
    public IDataStreamInfo deserialize(ResourceFormat format, InputStream is) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Iterator<? extends IDataStreamInfo> deserializeArray(ResourceFormat format, InputStream is) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void serialize(DataStreamKey key, IDataStreamInfo res, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        serializeAsJson(key, res, writer);
        writer.flush();
    }


    @Override
    public void serialize(Stream<Entry<DataStreamKey, IDataStreamInfo>> results, Collection<ResourceLink> links, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        
        writer.beginArray();
        results.forEach(entry -> {
            try
            {
                serializeAsJson(entry.getKey(), entry.getValue(), writer);
            }
            catch (IOException e)
            {
                IOException wrappedEx = new IOException("Error writing datastream JSON", e);
                throw new IllegalStateException(wrappedEx);
            }
        });
        writer.endArray();
        writer.flush();
    }
    
    
    protected void serializeAsJson(DataStreamKey key, IDataStreamInfo dsInfo, JsonWriter writer) throws IOException
    {
        writer.beginObject();
        
        writer.name("id").value(Long.toString(getExternalID(key.getInternalID()), 36));
        writer.name("name").value(dsInfo.getName());
        
        writer.name("validTime").beginArray()
            .value(dsInfo.getValidTime().begin().toString())
            .value(dsInfo.getValidTime().end().toString())
            .endArray();

        if (dsInfo.getPhenomenonTimeRange() != null)
        {
            writer.name("phenomenonTime").beginArray()
                .value(dsInfo.getPhenomenonTimeRange().begin().toString())
                .value(dsInfo.getPhenomenonTimeRange().end().toString())
                .endArray();
        }

        if (dsInfo.getResultTimeRange() != null)
        {
            writer.name("resultTime").beginArray()
                .value(dsInfo.getResultTimeRange().begin().toString())
                .value(dsInfo.getResultTimeRange().end().toString())
                .endArray();
        }
        
        if (dsInfo.hasDiscreteResultTimes())
        {
            writer.name("runTimes").beginArray();
            for (var time: dsInfo.getDiscreteResultTimes().keySet())
                writer.value(time.toString());
            writer.endArray();
        }
        
        // result structure
        try
        {
            SWEJsonStreamWriter sweWriter = new SWEJsonStreamWriter(writer);
            SWEStaxBindings sweBindings = new SWEStaxBindings();
            writer.name("resultSchema");
            sweBindings.writeDataComponent(sweWriter, dsInfo.getRecordStructure(), false);
        }
        catch (Exception e)
        {
            throw new IOException("Error writing SWE Common result structure", e);
        }
        
        // available encodings
        
        writer.endObject();
    }
}
