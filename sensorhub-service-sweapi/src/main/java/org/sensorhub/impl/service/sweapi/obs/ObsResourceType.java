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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.impl.service.sweapi.IdUtils;
import org.sensorhub.impl.service.sweapi.resource.PropertyFilter;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.resource.ResourceType;
import org.vast.swe.fast.JsonDataWriter;
import com.google.gson.stream.JsonWriter;


public class ObsResourceType extends ResourceType<BigInteger, IObsData>
{
    public static final int EXTERNAL_ID_SEED = 918742953;
    private static final byte[] RESULT_INDENT;
    
    IObsStore obsStore;
    
    static
    {
        RESULT_INDENT = new byte[INDENT.length()*2];
        Arrays.fill(RESULT_INDENT, (byte)' ');
    }
    
    
    ObsResourceType(IObsStore obsStore)
    {
        super(new IdUtils(EXTERNAL_ID_SEED));
        this.obsStore = obsStore;
    }
    
    
    public BigInteger getInternalID(BigInteger externalID)
    {
        return externalID;
    }
    
    
    public BigInteger getExternalID(BigInteger internalID)
    {
        return internalID;
    }


    @Override
    public IObsData deserialize(ResourceFormat format, InputStream is) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public Iterator<? extends IObsData> deserializeArray(ResourceFormat format, InputStream is) throws IOException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void serialize(BigInteger key, IObsData obs, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        var resultWriter = getSweJsonWriter(obs.getDataStreamID(), os, propFilter);
        serializeAsJson(key, obs, writer, resultWriter);
        writer.flush();
    }


    @Override
    public void serialize(Stream<Entry<BigInteger, IObsData>> results, Collection<ResourceLink> links, PropertyFilter propFilter, ResourceFormat format, OutputStream os) throws IOException
    {
        var writer = getJsonWriter(os, propFilter);
        writer.beginArray();
        
        long dsID = 0;
        JsonDataWriter resultWriter = null;
        var resultIt = results.iterator();
        while (resultIt.hasNext())
        {
            try
            {
                var entry = resultIt.next();
                var obs = entry.getValue();
                
                if (dsID != obs.getDataStreamID())
                {
                    dsID = obs.getDataStreamID();
                    resultWriter = getSweJsonWriter(dsID, os, propFilter);
                }
                
                serializeAsJson(entry.getKey(), obs, writer, resultWriter);
            }
            catch (IOException e)
            {
                IOException wrappedEx = new IOException("Error writing observation JSON", e);
                throw new IllegalStateException(wrappedEx);
            }
        }
        
        writer.endArray();
        writer.flush();
    }
    
    
    protected JsonDataWriter getSweJsonWriter(long dsID, OutputStream os, PropertyFilter propFilter)
    {
        var dsInfo = obsStore.getDataStreams().get(new DataStreamKey(dsID));
        var dataWriter = new JsonDataWriter();
        dataWriter.setDataComponents(dsInfo.getRecordStructure());
        /*dataWriter.setDataComponentFilter(new IComponentFilter() {
            @Override
            public boolean accept(DataComponent comp)
            {
                if (comp.getParent() == null ||
                    SWEConstants.DEF_PHENOMENON_TIME.equals(comp.getDefinition()) ||
                    SWEConstants.DEF_SAMPLING_TIME.equals(comp.getDefinition()))
                    return false;
                else
                    return true;
            }            
        });*/
        // use custom output stream to insert indent
        dataWriter.setOutput(new FilterOutputStream(os) {
            public void write(int b) throws IOException
            {
                super.write(b);                
                if (((char)b) == '\n')
                    super.write(RESULT_INDENT);
            }            
        });
        return dataWriter;
    }
    
    
    protected void serializeAsJson(BigInteger key, IObsData obs, JsonWriter writer, JsonDataWriter resultWriter) throws IOException
    {
        writer.beginObject();
        
        writer.name("id").value(key.toString(ResourceType.ID_RADIX));
        
        var externalDsId = idUtils.getExternalID(obs.getDataStreamID());
        writer.name("datastream").value(Long.toString(externalDsId, ResourceType.ID_RADIX));
        
        if (obs.getFoiID() != null && obs.getFoiID() != FeatureId.NULL_FEATURE)
        {
            var externalfoiId = idUtils.getExternalID(obs.getFoiID().getInternalID());
            writer.name("foi").value(Long.toString(externalfoiId, ResourceType.ID_RADIX));      
        }
        
        writer.name("phenomenonTime").value(obs.getPhenomenonTime().toString());
        writer.name("resultTime").value(obs.getResultTime().toString());
        
        writer.name("result").jsonValue("");
        writer.flush();
        resultWriter.write(obs.getResult());
        resultWriter.flush();
        
        writer.endObject();
    }
}
