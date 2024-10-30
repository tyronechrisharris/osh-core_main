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
import java.util.Collection;
import javax.xml.stream.XMLStreamException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.service.sweapi.ResourceParseException;
import org.sensorhub.impl.service.sweapi.SWECommonUtils;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamReader;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import net.opengis.swe.v20.JSONEncoding;


public class DataStreamSchemaBindingSweCommon extends ResourceBindingJson<DataStreamKey, IDataStreamInfo>
{
    String rootURL;
    ResourceFormat obsFormat;
    SWEStaxBindings sweBindings;
    SWEJsonStreamReader sweReader;
    SWEJsonStreamWriter sweWriter;
    
    
    DataStreamSchemaBindingSweCommon(ResourceFormat obsFormat, RequestContext ctx, IdEncoders idEncoders, boolean forReading) throws IOException
    {
        super(ctx, idEncoders, forReading);
        init(obsFormat, ctx, forReading);
    }
    
    
    DataStreamSchemaBindingSweCommon(ResourceFormat obsFormat, RequestContext ctx, IdEncoders idEncoders, JsonReader reader) throws IOException
    {
        super(ctx, idEncoders, reader);
        init(obsFormat, ctx, true);
    }
    
    
    void init(ResourceFormat obsFormat, RequestContext ctx, boolean forReading)
    {
        this.rootURL = ctx.getApiRootURL();
        this.obsFormat = obsFormat;
        this.sweBindings = new SWEStaxBindings();
        
        if (forReading)
            this.sweReader = new SWEJsonStreamReader(reader);
        else
            this.sweWriter = new SWEJsonStreamWriter(writer);
    }
    
    
    @Override
    public IDataStreamInfo deserialize(JsonReader reader) throws IOException
    {
        DataComponent resultStruct = null;
        DataEncoding resultEncoding = new TextEncodingImpl();
        
        try
        {
            // read BEGIN_OBJECT only if not already read by caller
            // this happens when reading embedded schema and auto-detecting obs format
            if (reader.peek() == JsonToken.BEGIN_OBJECT)
                reader.beginObject();
            
            while (reader.hasNext())
            {
                var prop = reader.nextName();
                
                if ("recordSchema".equals(prop))
                {
                    sweReader.nextTag();
                    resultStruct = sweBindings.readDataComponent(sweReader);
                    resultStruct.setName(SWECommonUtils.NO_NAME);
                }
                else if ("recordEncoding".equals(prop))
                {
                    sweReader.nextTag();
                    resultEncoding = sweBindings.readAbstractEncoding(sweReader);
                }
                else
                    reader.skipValue();
            }
            reader.endObject();
        }
        catch (XMLStreamException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        catch (IllegalStateException e)
        {
            throw new ResourceParseException(INVALID_JSON_ERROR_MSG + e.getMessage());
        }
        
        var dsInfo = new DataStreamInfo.Builder()
            .withName(SWECommonUtils.NO_NAME) // name will be set later
            .withSystem(FeatureId.NULL_FEATURE) // System ID will be set later
            .withRecordDescription(resultStruct)
            .withRecordEncoding(resultEncoding)
            .build();
        
        return dsInfo;
    }


    @Override
    public void serialize(DataStreamKey key, IDataStreamInfo dsInfo, boolean showLinks, JsonWriter writer) throws IOException
    {
        var dsId = idEncoders.getDataStreamIdEncoder().encodeID(key.getInternalID());
        var sweEncoding = SWECommonUtils.getEncoding(dsInfo.getRecordStructure(), dsInfo.getRecordEncoding(), obsFormat);
        
        writer.beginObject();
        writer.name("datastream@id").value(dsId);
        writer.name("obsFormat").value(obsFormat.toString());
        
        // result structure & encoding
        try
        {
            writer.name("recordSchema");
            sweWriter.resetContext();
            sweBindings.writeDataComponent(sweWriter, dsInfo.getRecordStructure(), false);
        }
        catch (Exception e)
        {
            throw new IOException("Error writing SWE Common record structure", e);
        }
        
        try
        {
            if (!(sweEncoding instanceof JSONEncoding))
            {
                writer.name("recordEncoding");
                sweWriter.resetContext();
                sweBindings.writeAbstractEncoding(sweWriter, sweEncoding);
            }
        }
        catch (Exception e)
        {
            throw new IOException("Error writing SWE Common record encoding", e);
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
