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
import java.util.ArrayList;
import org.jglue.fluentjson.JsonBuilderFactory;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.common.SensorHubException;
import com.google.gson.JsonObject;


public class TestSweApiFois extends TestSweApiBase
{
    TestSweApiSystems systemTests = new TestSweApiSystems();
    
    
    @Before
    public void setup() throws IOException, SensorHubException
    {
        super.setup();
        systemTests.swaRootUrl = swaRootUrl;
    }
    
    
    @Test
    public void testAddSystemAndFois() throws Exception
    {
        // add system
        var sysUrl = systemTests.addSystem(10);
        
        // add foi
        int numFois = 10;
        var ids = new ArrayList<String>();
        for (int i = 0; i < numFois; i++)
        {
            var url = addFoi(sysUrl, i);
            var id = url.substring(url.lastIndexOf('/')+1);
            ids.add(id);
        }
        
        // get list of fois
        var jsonResp = sendGetRequestAndParseJson(concat(sysUrl, FOI_COLLECTION));
        checkCollectionItemIds(ids, jsonResp);
    }
    
    
    @Test
    public void testAddSystemAndFoiBatch() throws Exception
    {
        // add system
        var sysUrl = systemTests.addSystem(10);
        
        // add foi
        int numFois = 10;
        var ids = new ArrayList<String>();
        for (int i = 0; i < numFois; i++)
        {
            var url = addFoi(sysUrl, i);
            var id = url.substring(url.lastIndexOf('/')+1);
            ids.add(id);
        }
        
        // get list of fois
        var jsonResp = sendGetRequestAndParseJson(concat(sysUrl, FOI_COLLECTION));
        checkCollectionItemIds(ids, jsonResp);
    }
    
    
    protected String addFoi(String sysUrl, int num) throws Exception
    {
        // add foi
        var json = createFoiGeoJson(num);
        var httpResp = sendPostRequest(concat(sysUrl, FOI_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequestAndParseJson(url);
        //System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertFoiEquals(json, (JsonObject)jsonResp);
        
        return url;
    }
    
    
    
    
    protected JsonObject createFoiGeoJson(int fNum) throws Exception
    {
        var json = JsonBuilderFactory.buildObject()
            .add("type", "Feature")
            .addObject("geometry")
                .add("type", "Point")
                .addArray("coordinates")
                    .add(30.56871)
                    .add(58.479315)
                .end()
            .end()
            .addObject("properties")
                .add("uid", String.format("urn:osh:foi:test%03d", fNum))
                .add("name", "Sampling Feature #" + fNum)
                .add("description", "Sensor Station #" + fNum)
                .add("featureType", "http://www.opengis.net/def/featureType/MyFeature")
            .end();
        
        return json.getJson();
    }
    
    
    protected void assertFoiEquals(JsonObject expected, JsonObject actual)
    {
        // remove some fields not present in POST request before comparison
        actual.remove("id");
        actual.remove("links");
        assertEquals(expected, actual);
    }
    
}
