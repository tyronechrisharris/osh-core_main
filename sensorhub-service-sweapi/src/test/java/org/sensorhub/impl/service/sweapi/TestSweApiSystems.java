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
import static org.junit.Assert.assertTrue;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.jglue.fluentjson.JsonBuilderFactory;
import org.junit.Test;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class TestSweApiSystems extends TestSweApiBase
{
    public static final String UID_FORMAT = "urn:osh:proc:test%03d";
    
    
    public static class SystemInfo
    {
        public String url;
        public String id;
        public Collection<SystemInfo> subsystems = new ArrayList<>();
        public Collection<String> fois = new ArrayList<>();
        public Collection<String> datastreams = new ArrayList<>();
        public Collection<String> controls = new ArrayList<>();
        
        public SystemInfo(String url)
        {
            this.url = url;
            this.id = getResourceId(url);
        }
    }
    
    
    @Test
    public void testAddSystemAndGetById() throws Exception
    {
        addSystem(1, true);
        addSystem(10, true);
    }
    
    
    @Test
    public void testAddSystemBatchAndGet() throws Exception
    {
        addSystemBatch(1, 20, true);
    }
    
    
    @Test
    public void testAddSystemBatchAndGetAllById() throws Exception
    {
        var urlList = addSystemBatch(100, 110);
        var idList = Lists.transform(urlList, TestSweApiBase::getResourceId);
        
        // get request with full id list
        setDebug(true);
        var jsonResp = sendGetRequestAndParseJson(SYSTEM_COLLECTION + "?id=" + String.join(",", idList));
        checkCollectionItemIds(idList, jsonResp);
        
        // get request with subset of id list
        setDebug(true);
        var subList = idList.subList(3, 9);
        jsonResp = sendGetRequestAndParseJson(SYSTEM_COLLECTION + "?id=" + String.join(",", subList));
        checkCollectionItemIds(subList, jsonResp);
    }
    
    
    @Test
    public void testGetSystemWrongId() throws Exception
    {
        var urlInvalidId = concat(SYSTEM_COLLECTION, "toto");
        sendGetRequestAndCheckStatus(urlInvalidId, 404);
        
        addSystem(1, false);
        sendGetRequestAndCheckStatus(urlInvalidId, 404);
    }
    
    
    @Test
    public void testAddSystemCustomPropsAndGetById() throws Exception
    {
        addSystem(5, true, ImmutableMap.<String, Object>builder()
            .put("description", "Human readable system description")
            .put("num_prop1", 53)
            .put("str_prop2", "string")
            .build());
    }
    
    
    public void testAddDuplicateSystem() throws Exception
    {
        var json = createSystemGeoJson(1);
        var resp = sendPostRequest(SYSTEM_COLLECTION, json);
        assertEquals(201, resp.statusCode());
        resp = sendPostRequest(SYSTEM_COLLECTION, json);
        assertEquals(400, resp.statusCode());
    }
    
    
    @Test
    public void testAddSystemsAndGetByUid() throws Exception
    {
        var uid1 = String.format(UID_FORMAT, 1);
        var url1 = addSystem(1, false);
        
        var uid2 = String.format(UID_FORMAT, 5);
        var url2 = addSystem(5, false);
        
        // get 1 at a time
        // test json resources fetched by ID and by UID are the same!
        var json = (JsonObject)sendGetRequestAndParseJson(url1);
        var items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?uid=" + uid1, 1);
        checkId(url1, items.get(0));
        checkSystemUid(uid1, items.get(0));
        json.remove("links");
        assertEquals(json, items.get(0));
        
        json = (JsonObject)sendGetRequestAndParseJson(url2);
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?uid=" + uid2, 1);
        checkId(url2, items.get(0));
        checkSystemUid(uid2, items.get(0));
        json.remove("links");
        assertEquals(json, items.get(0));
        
        // get both
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?uid=" + uid1 + "," + uid2, 2);
        checkId(url1, items.get(0));
        checkSystemUid(uid1, items.get(0));
        checkId(url2, items.get(1));
        checkSystemUid(uid2, items.get(1));
    }
    
    
    @Test
    public void testAddSystemsAndFilterByProps() throws Exception
    {
        var uid1 = String.format(UID_FORMAT, 1);
        var url1 = addSystem(1, false, ImmutableMap.<String, Object>builder()
            .put("prop1", 53)
            .put("prop2", "temp")
            .build());
        
        var uid2 = String.format(UID_FORMAT, 2);
        var url2 = addSystem(2, false, ImmutableMap.<String, Object>builder()
            .put("prop1", 22)
            .put("prop2", "pressure")
            .build());
        
        var uid3 = String.format(UID_FORMAT, 3);
        var url3 = addSystem(3, false, ImmutableMap.<String, Object>builder()
            .put("prop1", 53)
            .put("prop2", "pressure")
            .build());
        
        // match one
        var items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop1=22", 1);
        checkId(url2, items.get(0));
        checkSystemUid(uid2, items.get(0));
        checkFeatureProp("prop1", Double.valueOf(22), items.get(0));
        
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop2=temp", 1);
        checkId(url1, items.get(0));
        checkSystemUid(uid1, items.get(0));
        checkFeatureProp("prop2", "temp", items.get(0));
        
        // match one with AND
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop2=pressure&p:prop1=53", 1);
        checkId(url3, items.get(0));
        checkSystemUid(uid3, items.get(0));
        checkFeatureProp("prop1", Double.valueOf(53), items.get(0));
        checkFeatureProp("prop2", "pressure", items.get(0));

        // match several
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop2=pressure", 2);
        checkHasIds(items, url2, url3);
        
        // match several with OR
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop2=pressure,temp", 3);
        checkHasIds(items, url1, url2, url3);
        
        // match several with wildcard
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop2=press*", 2);
        checkHasIds(items, url2, url3);
        
        // match none
        sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?p:prop2=nothing", 0);
    }
    
    
    @Test
    public void testAddSystemsAndFilterByKeywords() throws Exception
    {
        var url1 = addSystem(1, false, ImmutableMap.<String, Object>builder()
            .put("description", "this is a thermometer")
            .build());
        
        var url2 = addSystem(2, false, ImmutableMap.<String, Object>builder()
            .put("description", "this is a barometer")
            .build());
        
        var url3 = addSystem(3, false, ImmutableMap.<String, Object>builder()
            .put("description", "this is a camera")
            .build());
        
        // match one
        var items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?q=thermo", 1);
        checkId(url1, items.get(0));
        
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?q=camera", 1);
        checkId(url3, items.get(0));
        
        // match several with OR
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?q=thermo,baro", 2);
        checkHasIds(items, url1, url2);
        
        // match none
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?q=borehole", 0);
    }
    
    
    @Test
    public void testUpdateSystemCustomPropsAndGetById() throws Exception
    {
        var url1 = addSystem(5, true, ImmutableMap.<String, Object>builder()
            .put("description", "This is my first system")
            .put("num_prop1", 53)
            .put("str_prop2", "string")
            .build());
        
        var url2 = addSystem(23, true, ImmutableMap.<String, Object>builder()
            .put("description", "This is system 2")
            .put("size", 21)
            .put("mode", "turbo")
            .build());
        
        updateSystem(url1, 5, true, ImmutableMap.<String, Object>builder()
            .put("description", "Updated version of first system")
            .put("num_prop1", 15)
            .put("str_prop2", "text")
            .build());
        
        updateSystem(url2, 23, true, ImmutableMap.<String, Object>builder()
            .put("description", "Updated version of system 2")
            .put("size", 22)
            .put("mode", "turbo")
            .put("str_prop2", "text")
            .build());
    }
    
    
    @Test
    public void testUpdateSystemWrongId() throws Exception
    {
        addSystem(55, true, ImmutableMap.<String, Object>builder()
            .put("description", "This is my first system")
            .put("num_prop1", 53)
            .put("str_prop2", "string")
            .build());
        
        var urlInvalidId = concat(SYSTEM_COLLECTION, "917pe5d33noso");
        sendPutRequestAndCheckStatus(urlInvalidId, new JsonObject(), 404);
        
        var resp = updateSystem(urlInvalidId, 55, false, ImmutableMap.<String, Object>builder()
            .put("description", "Updated version of first system")
            .put("num_prop1", 15)
            .put("str_prop2", "text")
            .build());
        assertEquals(404, resp.statusCode());
    }
    
    
    @Test
    public void testUpdateSystemUidChanged() throws Exception
    {
        var url1 = addSystem(55, true, ImmutableMap.<String, Object>builder()
            .put("description", "This is my first system")
            .put("num_prop1", 53)
            .put("str_prop2", "string")
            .build());
        
        var resp = updateSystem(url1, 22, false, ImmutableMap.<String, Object>builder()
            .put("description", "Updated version of first system")
            .put("num_prop1", 15)
            .put("str_prop2", "text")
            .build());
        assertEquals(400, resp.statusCode());
    }
    
    
    @Test
    public void testAddSystemAndDeleteById() throws Exception
    {
        var url1 = addSystem(1, false);
        var url2 = addSystem(10, false);
        
        var items = sendGetRequestAndGetItems(SYSTEM_COLLECTION, 2);
        checkHasIds(items, url1, url2);
        
        sendDeleteRequestAndCheckStatus(url1, 204);
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION, 1);
        checkHasIds(items, url2);
        
        sendDeleteRequestAndCheckStatus(url2, 204);
        items = sendGetRequestAndGetItems(SYSTEM_COLLECTION, 0);
    }
    
    
    @Test
    public void testDeleteSystemWrongId() throws Exception
    {
        var urlInvalidId = concat(SYSTEM_COLLECTION, "toto");
        sendDeleteRequestAndCheckStatus(urlInvalidId, 404);
        
        var url1 = addSystem(1, false);
        
        sendDeleteRequestAndCheckStatus(urlInvalidId, 404);
        sendDeleteRequestAndCheckStatus(url1, 204);
        
        // delete again and check it doesn't exist anymore
        sendDeleteRequestAndCheckStatus(url1, 404);
    }

    
    @Test
    public void testAddSystemDetailsAndGet() throws Exception
    {
        var json = createSystemSml(1);
        var httpResp = sendPostRequest(SYSTEM_COLLECTION, json, ResourceFormat.SML_JSON.getMimeType());
        var url = getLocation(httpResp);
        
        // get summary
        var jsonResp = (JsonObject)sendGetRequestAndParseJson(url);
        checkId(url, jsonResp);
        assertEquals(json.getAsJsonArray("names").get(0).getAsString(), jsonResp.getAsJsonObject("properties").get("name").getAsString());
        assertEquals(json.get("description").getAsString(), jsonResp.getAsJsonObject("properties").get("description").getAsString());
        
        // get details
        jsonResp = (JsonObject)sendGetRequestAndParseJson(concat(url, "/details"));
        checkId(url, jsonResp);
        jsonResp.remove("id"); // remove auto-assigned id before compare
        jsonResp.remove("validTimes");
        assertEquals(json, jsonResp);
    }
    
    
    @Test
    public void testAddSystemMembersAndGetById() throws Exception
    {
        // add system group
        var groupUrl = addSystem(1);
        
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
    }
    
    
    @Test
    public void testAddSystemMembersDepth2AndGet() throws Exception
    {
        var sys1 = addSystemAndSubsystems(1, new int[] {5, 5}, 10);
        var sys2 = addSystemAndSubsystems(2, new int[] {6, 3}, 10);
        
        checkSubSystemIds(sys1);
        checkSubSystemIds(sys2);
        
        // TODO test with searchMembers=true
    }
    
    
    @Test
    public void testAddSystemMembersDepth3AndGet() throws Exception
    {
        var sys1 = addSystemAndSubsystems(1, new int[] {2, 2, 2}, 10);
        var sys2 = addSystemAndSubsystems(3, new int[] {3, 3, 2}, 10);
        var sys3 = addSystemAndSubsystems(5, new int[] {5, 4}, 10);
        
        checkSubSystemIds(sys1);
        checkSubSystemIds(sys2);
        checkSubSystemIds(sys3);
        
        // TODO test with searchMembers=true
    }
    
    
    @Test
    public void testAddSystemMembersAndCheckCannotDeleteParent() throws Exception
    {
        var sys1 = addSystemAndSubsystems(1, new int[] {2, 2, 2}, 10);
        
        sendDeleteRequestAndCheckStatus(sys1.url, 400);
        
        // check system and subsystems have NOT been deleted
        sendGetRequestAndCheckStatus(sys1.url, 200);
        sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?searchMembers=true", 1+2+2*2+2*2*2);
    }
    
    
    @Test
    public void testAddSystemMembersAndDeleteCascade() throws Exception
    {
        var sys1 = addSystemAndSubsystems(1, new int[] {2, 2, 2}, 10);
        
        sendDeleteRequestAndCheckStatus(sys1.url + "?cascade=true", 204);
        
        // check system and subsystems have been deleted
        sendGetRequestAndCheckStatus(sys1.url, 404);
        sendGetRequestAndGetItems(SYSTEM_COLLECTION + "?searchMembers=true", 0);
    }
    
    
    
    protected JsonObject createSystemGeoJson(int procNum) throws Exception
    {
        return createSystemGeoJson(procNum, Collections.emptyMap());
    }
    
    
    protected JsonObject createSystemGeoJson(int procNum, Map<String, Object> props) throws Exception
    {
        var json = JsonBuilderFactory.buildObject()
            .add("type", "Feature")
            .addObject("properties")
              .add("uid", String.format(UID_FORMAT, procNum))
              .add("name", "Test Sensor #" + procNum);
        
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
    
    
    protected String addSystem(int num) throws Exception
    {
        return addSystem(num, false);
    }
    
    
    protected String addSystem(int num, boolean checkGet) throws Exception
    {
        return addSystem(num, checkGet, Collections.emptyMap());
    }
    
    
    protected String addSystem(int num, boolean checkGet, Map<String, Object> props) throws Exception
    {
        var json = createSystemGeoJson(num, props);
        
        var httpResp = sendPostRequest(SYSTEM_COLLECTION, json);
        var url = getLocation(httpResp);
        
        if (checkGet)
        {
            var jsonResp = sendGetRequestAndParseJson(url);
            checkId(url, jsonResp);
            assertSystemEquals(json, (JsonObject)jsonResp);
        }
        
        return url;
    }
    
    
    protected List<String> addSystemBatch(int startNum, int endNum) throws Exception
    {
        return addSystemBatch(startNum, endNum, false);
    }
    
    
    protected List<String> addSystemBatch(int startNum, int endNum, boolean checkGet) throws Exception
    {
        var array = new JsonArray();
        for (int num = startNum; num <= endNum; num++)
        {
            var json = createSystemGeoJson(num);
            array.add(json);
        }
        
        var urlList = sendPostRequestAndParseUrlList(SYSTEM_COLLECTION, array);
        assertEquals("Wrong number of resources created", array.size(), urlList.size());
        
        if (checkGet)
        {
            for (int i = 0; i < array.size(); i++)
            {
                var url = urlList.get(i);
                var json = array.get(i);
                var jsonResp = sendGetRequestAndParseJson(url);
                checkId(url, jsonResp);
                assertSystemEquals((JsonObject)json, (JsonObject)jsonResp);
            }
        }
        
        return urlList;
    }
    
    
    protected HttpResponse<String> updateSystem(String path, int num, boolean checkGet, Map<String, Object> props) throws Exception
    {
        var json = createSystemGeoJson(num, props);
        
        var resp = sendPutRequest(path, json);
        
        if (checkGet)
        {
            var jsonResp = sendGetRequestAndParseJson(path);
            checkId(path, jsonResp);
            assertSystemEquals(json, (JsonObject)jsonResp);
        }
        
        return resp;
    }
    
    
    protected JsonObject createSystemSml(int procNum) throws Exception
    {
        var numId = String.format("%03d", procNum);
        var sml = "{\n"
            + "  \"type\": \"PhysicalSystem\",\n"
            + "  \"description\": \"Sensor registered using SWE API\",\n"
            + "  \"identifier\": \"urn:osh:sensor:test:" + numId + "\",\n"
            + "  \"names\": [\"Test sensor\"],\n"
            + "  \"identifications\": [\n"
            + "    {\n"
            + "      \"type\": \"IdentifierList\",\n"
            + "      \"identifiers\": [\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:ogc:def:identifier:OGC:longname\",\n"
            + "          \"label\": \"Long Name\",\n"
            + "          \"value\": \"Test sensor " + numId + " located in my garden\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:ogc:def:identifier:OGC:shortname\",\n"
            + "          \"label\": \"Short Name\",\n"
            + "          \"value\": \"Test Sensor #" + numId + "\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:ogc:def:identifier:OGC:manufacturer\",\n"
            + "          \"label\": \"Manufacturer\",\n"
            + "          \"value\": \"SensorMakers Inc.\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"http://sensorml.com/ont/swe/property/SerialNumber\",\n"
            + "          \"label\": \"Serial Number\",\n"
            + "          \"value\": \"0123456879\"\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  ],\n"
            + "  \"classifications\": [\n"
            + "    {\n"
            + "      \"type\": \"ClassifierList\",\n"
            + "      \"classifiers\": [\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:ogc:def:classifier:OGC:application\",\n"
            + "          \"label\": \"Intended Application\",\n"
            + "          \"value\": \"weather\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"type\": \"Term\",\n"
            + "          \"definition\": \"urn:sensor:classifier:sensorType\",\n"
            + "          \"label\": \"Instrument Type\",\n"
            + "          \"codeSpace\": {\n"
            + "            \"href\": \"http://gcmdservices.gsfc.nasa.gov/static/kms/instruments/instruments.xml\"\n"
            + "          },\n"
            + "          \"value\": \"weather station\"\n"
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
    
    
    protected void assertSystemEquals(JsonObject expected, JsonObject actual)
    {
        actual.remove("id");
        actual.remove("links");
        assertEquals(expected, actual);
    }
    
    
    /*----------------*/
    /* Nested Systems */
    /*----------------*/
    
    protected SystemInfo addSystemAndSubsystems(int num, int[] levelSizes, int maxLevelSize) throws Exception
    {
        int mul = (int)Math.pow(10.0, Math.ceil(Math.log10(maxLevelSize)));
        
        var url = addSystem(num);
        var sysInfo = new SystemInfo(url);
        addSubsystems(sysInfo, 1, mul, num*mul, levelSizes);
        
        return sysInfo;
    }
    
    
    protected void addSubsystems(SystemInfo parent, int level, int mul, int offset, int[] levelSizes) throws Exception
    {
        for (int i = 0; i < levelSizes[0]; i++)
        {
            var idx = i + offset + 1;
            var url = addMember(parent.url, idx);
            var subsysInfo = new SystemInfo(url);
            System.err.println("Added " + url);
            
            // add nested systems if not lowest level
            if (level < levelSizes.length)
            {
                int nextOffset = idx*mul;
                addSubsystems(subsysInfo, level+1, mul, nextOffset, levelSizes);
            }
            
            parent.subsystems.add(subsysInfo);
        }
    }
    
    
    protected String addMember(String parentUrl, int num) throws Exception
    {
        // add group member
        var json = createSystemGeoJson(num);
        var httpResp = sendPostRequest(concat(parentUrl, MEMBER_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequestAndParseJson(url);
        //System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertSystemEquals(json, (JsonObject)jsonResp);
        
        return url;
    }
    
    
    protected void checkSubSystemIds(SystemInfo sys) throws Exception
    {
        var jsonResp = sendGetRequestAndParseJson(concat(sys.url, MEMBER_COLLECTION));
        var expectedIds = Collections2.transform(sys.subsystems, s -> s.id);
        checkCollectionItemIds(expectedIds, jsonResp);
        
        for (var subsys: sys.subsystems)
            checkSubSystemIds(subsys);
    }
    
    
    protected void checkSystemUid(int sysNum, JsonElement actual)
    {
        checkSystemUid(String.format(UID_FORMAT, sysNum), actual);
    }
    
    
    protected void checkSystemUid(String expectedUid, JsonElement actual)
    {
        assertTrue(actual instanceof JsonObject);
        var uid = ((JsonObject)actual).getAsJsonObject("properties").get("uid").getAsString();
        assertEquals(expectedUid, uid);
    }
    
}
