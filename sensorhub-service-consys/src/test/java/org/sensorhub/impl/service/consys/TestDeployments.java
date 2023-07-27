/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.jglue.fluentjson.JsonBuilderFactory;
import org.vast.util.TimeExtent;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class TestDeployments extends AbstractTestAllSmlFeatures
{
    public static final String UID_FORMAT = "urn:osh:deplyt:test%03d";
    
    
    public static class DeploymentInfo
    {
        public String url;
        public String id;
        public Collection<DeploymentInfo> systems = new ArrayList<>();
        public Collection<String> fois = new ArrayList<>();
        
        public DeploymentInfo(String url)
        {
            this.url = url;
            this.id = getResourceId(url);
        }
    }
    
    
    public TestDeployments()
    {
        super(DEPLOYMENT_COLLECTION, UID_FORMAT);
    }
    
    
    /*@Test
    public void testAddSystemMembersAndGetById() throws Exception
    {
        // add system group
        var groupUrl = addFeature(1);
        
        // add members
        int numMembers = 10;
        var ids = new ArrayList<String>();
        for (int i = 0; i < numMembers; i++)
        {
            var url = addMember(groupUrl, i+2);
            var id = getResourceId(url);
            ids.add(id);
        }
        
        // get list of members
        var jsonResp = sendGetRequestAndParseJson(concat(groupUrl, MEMBER_COLLECTION));
        checkCollectionItemIds(ids, jsonResp);
    }*/
    
    
    // Non-Test helper methods
    
    @Override
    protected JsonObject createFeatureGeoJson(int procNum, TimeExtent validTime, Map<String, Object> props) throws Exception
    {
        var json = JsonBuilderFactory.buildObject()
            .add("type", "Feature")
            .addObject("properties")
              .add("uid", String.format(UID_FORMAT, procNum))
              .add("name", "Test Deployment #" + procNum);
        
        if (validTime != null)
        {
            json.addArray("validTime")
                .add(validTime.begin().toString())
                .add(validTime.endsNow() ? "now" : validTime.end().toString())
            .end();
        }
        
        // add all other properties
        for (var prop: props.entrySet())
        {
            var val = prop.getValue();
            if (val instanceof String)
                json.add(prop.getKey(), (String)val);
            else if (val instanceof Number)
                json.add(prop.getKey(), (Number)val);
            else
                throw new IllegalArgumentException();
        }
        
        return json.end().getJson();
    }
    
    
    @Override
    protected JsonObject createFeatureSmlJson(int procNum) throws Exception
    {
        var numId = String.format("%03d", procNum);
        var sml = "{\n"
            + "  \"type\": \"Deployment\",\n"
            + "  \"description\": \"Deployment registered using CONSYS API\",\n"
            + "  \"identifier\": \"urn:osh:deplyt:test:" + numId + "\",\n"
            + "  \"names\": [\"Test deployment\"],\n"
            + "  \"identifications\": [\n"
            + "    {\n"
            + "      \"type\": \"IdentifierList\",\n"
            + "      \"identifiers\": [\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:ogc:def:identifier:OGC:shortname\",\n"
            + "          \"label\": \"Short Name\",\n"
            + "          \"value\": \"Artic Mission #" + numId + "\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:ogc:def:identifier:OGC:manufacturer\",\n"
            + "          \"label\": \"Mission ID\",\n"
            + "          \"value\": \"SD-1405\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"positions\": [\n"
            + "    {\n"
            + "      \"type\": \"Point\",\n"
            + "      \"id\": \"stationLocation\",\n"
            + "      \"srsName\": \"http://www.opengis.net/def/crs/EPSG/0/4979\",\n"
            + "      \"srsDimension\": \"3\",\n"
            + "      \"pos\": \"1.2311 43.5678 0\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
        
        return (JsonObject)JsonParser.parseString(sml);
    }
    
    
    @Override
    protected void assertFeatureEquals(JsonObject expected, JsonObject actual)
    {
        actual.remove("id");
        actual.remove("links");
        assertEquals(expected, actual);
    }
    
    
    protected String addMember(String parentUrl, int num) throws Exception
    {
        // add group member
        var json = createFeatureGeoJson(num);
        var httpResp = sendPostRequest(concat(parentUrl, MEMBER_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequestAndParseJson(url);
        //System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertFeatureEquals(json, (JsonObject)jsonResp);
        
        return url;
    }
    
}
