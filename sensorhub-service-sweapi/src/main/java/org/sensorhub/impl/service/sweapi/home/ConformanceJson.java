/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.home;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import org.sensorhub.impl.service.sweapi.SWEApiServiceConfig;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingJson;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.vast.util.Asserts;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


public class ConformanceJson extends ResourceBindingJson<Long, SWEApiServiceConfig>
{
    Collection<String> confClasses;
    
    
    public ConformanceJson(RequestContext ctx, Set<String> confClasses) throws IOException
    {
        super(ctx, null, false);
        this.confClasses = Asserts.checkNotNullOrEmpty(confClasses, "ConformanceClasses");
    }
    
    
    @Override
    public void serialize(Long key, SWEApiServiceConfig config, boolean showLinks, JsonWriter writer) throws IOException
    {
        writer.beginObject();
        writer.name("conformsTo").beginArray();
        for (var uri: confClasses)
            writer.value(uri);
        writer.endArray();
        writer.endObject();
        writer.flush();
    }


    @Override
    public SWEApiServiceConfig deserialize(JsonReader reader) throws IOException
    {
        // this should never be called since home page is read-only
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public void startCollection() throws IOException
    {
        // this should never be called since home page is not a collection
        throw new UnsupportedOperationException();
    }


    @Override
    public void endCollection(Collection<ResourceLink> links) throws IOException
    {
        // this should never be called since home page is not a collection
        throw new UnsupportedOperationException();
    }
}
