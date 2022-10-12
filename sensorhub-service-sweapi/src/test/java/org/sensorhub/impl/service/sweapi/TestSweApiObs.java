/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.common.SensorHubException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;


public class TestSweApiObs extends TestSweApiBase
{
    TestSweApiSystems systemTests = new TestSweApiSystems();
    TestSweApiDatastreams datastreamTests = new TestSweApiDatastreams();
    
    
    @Before
    public void setup() throws IOException, SensorHubException
    {
        super.setup();
        systemTests.swaRootUrl = swaRootUrl;
        datastreamTests.swaRootUrl = swaRootUrl;
    }
    
    
    @Test
    public void testAddDatastreamAndObservations() throws Exception
    {
        // add system
        var procUrl = systemTests.addSystem(33);
        
        // add datastream
        var dsUrl = datastreamTests.addDatastreamOmJson(procUrl, 115);
        
        // add observations
        var now = Instant.now();
        int numObs = 10;
        var ids = new ArrayList<String>();
        for (int i = 0; i < numObs; i++)
        {
            var url = addObservation(dsUrl, now, i);
            var id = url.substring(url.lastIndexOf('/')+1);
            ids.add(id);
        }
        
        // get list of obs
        var jsonResp = sendGetRequestAndParseJson(concat(dsUrl, OBS_COLLECTION));
        checkCollectionItemIds(ids, jsonResp);
    }
    
    
    protected String addObservation(String dsUrl, Instant startTime, int num) throws Exception
    {
        return addObservation(dsUrl, startTime, 1000L, num);
    }
    
    
    protected String addObservation(String dsUrl, Instant startTime, long timeStepMillis, int num) throws Exception
    {
        // add datastream
        var json = createObservationNoFoi(startTime, timeStepMillis, num);
        var httpResp = sendPostRequest(concat(dsUrl, OBS_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequestAndParseJson(url);
        checkId(url, jsonResp);
        assertObservationsEquals(json, (JsonObject)jsonResp);
        
        // return datastream ID
        return url;
    }
    
    
    protected JsonObject createObservationNoFoi(Instant startTime, long timeStepMillis, int num) throws Exception
    {
        var buffer = new StringWriter();
        
        try (var writer = new JsonWriter(buffer))
        {
            writer.beginObject();
            
            var timeStamp = startTime.plusMillis(num*timeStepMillis).toString();
            writer.name("phenomenonTime").value(timeStamp.toString());
            writer.name("resultTime").value(timeStamp.toString());
            writer.name("result")
                .beginObject()
                .name("f1").value(num)
                .name("f2").value(true)
                .name("f3").value(String.format("text_%03d", num))
                .endObject();
            
            writer.endObject();
        }
        catch (Exception e)
        {
            throw new IOException("Error writing JSON observation", e);
        }
        
        return (JsonObject)JsonParser.parseString(buffer.getBuffer().toString());
    }
    
    
    protected void assertObservationsEquals(JsonObject expected, JsonObject actual)
    {
        // remove some fields not present in POST request before comparison
        actual.remove("id");
        actual.remove("datastream@id");
        assertEquals(expected, actual);
    }
}
