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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


public class DataStreamSchemaBindingJson extends ResourceBindingJson<DataStreamKey, IDataStreamInfo>
{
    String rootURL;
    SWEStaxBindings sweBindings;
    JsonReader reader;
    SWEJsonStreamReader sweReader;
    JsonWriter writer;
    SWEJsonStreamWriter sweWriter;
    
    
    DataStreamSchemaBindingJson(RequestContext ctx, IdEncoder idEncoder, boolean forReading) throws IOException
    {
        super(ctx, idEncoder);
        
        this.rootURL = ctx.getApiRootURL();
        this.sweBindings = new SWEStaxBindings();
        
        if (forReading)
        {
            InputStream is = new BufferedInputStream(ctx.getInputStream());
            this.reader = getJsonReader(is);
            this.sweReader = new SWEJsonStreamReader(reader);
        }
        else
        {
            this.writer = getJsonWriter(ctx.getOutputStream(), ctx.getPropertyFilter());
            this.sweWriter = new SWEJsonStreamWriter(writer);
        }
    }
    
    
    @Override
    public IDataStreamInfo deserialize() throws IOException
    {
        // not needed since we never create the schema separately
        throw new UnsupportedOperationException();
    }


    @Override
    public void serialize(DataStreamKey key, IDataStreamInfo dsInfo, boolean showLinks) throws IOException
    {
        var publicDsID = encodeID(key.getInternalID());
        
        writer.beginObject();
        
        writer.name("datastream").value(Long.toString(publicDsID, 36));
        writer.name("name").value(dsInfo.getName());
        
        if (dsInfo.getDescription() != null)
            writer.name("description").value(dsInfo.getDescription());
        
        // result structure & encoding
        try
        {
            writer.name("resultSchema");
            sweWriter.resetContext();
            sweBindings.writeDataComponent(sweWriter, dsInfo.getRecordStructure(), false);
            
            writer.name("resultEncoding");
            sweWriter.resetContext();
            sweBindings.writeAbstractEncoding(sweWriter, dsInfo.getRecordEncoding());
        }
        catch (Exception e)
        {
            throw new IOException("Error writing SWE Common result structure", e);
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
