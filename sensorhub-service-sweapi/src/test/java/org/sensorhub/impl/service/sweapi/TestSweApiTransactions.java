/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import org.jglue.fluentjson.JsonBuilderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.datastore.h2.MVObsSystemDatabaseConfig;
import org.sensorhub.impl.service.HttpServer;
import org.sensorhub.impl.service.HttpServerConfig;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEHelper;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamWriter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


public class TestSweApiTransactions
{
    static Logger log = LoggerFactory.getLogger(TestSweApiTransactions.class);
    static final String SYSTEM_COLLECTION = "systems";
    static final String MEMBER_COLLECTION = "members";
    static final String FOI_COLLECTION = "fois";
    static final String DATASTREAM_COLLECTION = "datastreams";
    static final String OBS_COLLECTION = "observations";
    
    static final int SERVER_PORT = 8888;
    static final long TIMEOUT = 10000;
    SensorHub hub;
    File dbFile;
    SWEApiService swa;
    IObsSystemDatabase db;
    String swaRootUrl;
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    
    @Before
    public void setup() throws IOException, SensorHubException
    {
        // use temp DB file
        dbFile = File.createTempFile("sweapi-db-", ".dat");
        dbFile.deleteOnExit();
        
        // get instance with in-memory DB
        hub = new SensorHub();
        hub.start();
        var moduleRegistry = hub.getModuleRegistry();
        
        // start HTTP server
        HttpServerConfig httpConfig = new HttpServerConfig();
        httpConfig.httpPort = SERVER_PORT;
        var httpServer = (HttpServer)moduleRegistry.loadModule(httpConfig, TIMEOUT);
        
        // start DB
        MVObsSystemDatabaseConfig dbCfg = new MVObsSystemDatabaseConfig();
        dbCfg.storagePath = dbFile.getAbsolutePath();
        dbCfg.databaseNum = 2;
        dbCfg.readOnly = false;
        dbCfg.name = "SWE API Database";
        dbCfg.autoStart = true;
        db = (IObsSystemDatabase)moduleRegistry.loadModule(dbCfg, TIMEOUT);
        ((IModule<?>)db).waitForState(ModuleState.STARTED, TIMEOUT);
        
        // start SensorThings service
        SWEApiServiceConfig swaCfg = new SWEApiServiceConfig();
        swaCfg.databaseID = dbCfg.id;
        swaCfg.endPoint = "/api";
        swaCfg.name = "SWE API Service";
        swaCfg.autoStart = true;
        swa = (SWEApiService)moduleRegistry.loadModule(swaCfg, TIMEOUT);
        swaRootUrl = httpServer.getPublicEndpointUrl(swaCfg.endPoint);
    }

    
    @Test
    public void testAddSystemAndGet() throws Exception
    {
        var json = createSystemGeoJson(1);
        
        var httpResp = sendPostRequest(SYSTEM_COLLECTION, json);
        var url = getLocation(httpResp);
        assertNotNull(url);
        
        var jsonResp = sendGetRequest(url);
        System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertSystemEquals(json, (JsonObject)jsonResp);
    }
    
    
    @Test(expected = IOException.class)
    public void testAddDuplicateSystem() throws Exception
    {
        var json = createSystemGeoJson(1);        
        sendPostRequest(SYSTEM_COLLECTION, json);
        sendPostRequest(SYSTEM_COLLECTION, json);
    }
    
    
    protected JsonObject createSystemGeoJson(int procNum) throws Exception
    {
        var json = JsonBuilderFactory.buildObject()
            .add("type", "Feature")
            .addObject("properties")
              .add("uid", String.format("urn:osh:proc:test%03d", procNum))
              .add("name", "Test Sensor #" + procNum)
            .end();
        
        return json.getJson();
    }

    
    @Test
    public void testAddSystemDetailsAndGet() throws Exception
    {
        var json = createSystemSml(1);
        var httpResp = sendPostRequest(SYSTEM_COLLECTION, json, ResourceFormat.SML_JSON.getMimeType());
        var url = getLocation(httpResp);
        
        // get summary
        var jsonResp = (JsonObject)sendGetRequest(url);
        System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertEquals(json.getAsJsonArray("names").get(0).getAsString(), jsonResp.getAsJsonObject("properties").get("name").getAsString());
        assertEquals(json.get("description").getAsString(), jsonResp.getAsJsonObject("properties").get("description").getAsString());
        
        // get details
        jsonResp = (JsonObject)sendGetRequest(concat(url, "/details"));
        System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        jsonResp.remove("id"); // remove auto-assigned id before compare
        jsonResp.remove("validTimes");
        assertEquals(json, jsonResp);
    }
    
    
    protected String addSystem(int num) throws Exception
    {
        var json = createSystemGeoJson(num);
        var httpResp = sendPostRequest(SYSTEM_COLLECTION, json);
        return getLocation(httpResp);
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
    
    
    /*-------------------*/
    /* System Members */
    /*-------------------*/
    
    @Test
    public void testAddSystemMembersAndGet() throws Exception
    {
        // add system group
        var groupUrl = addSystem(1);
        
        // add members
        int numMembers = 10;
        var ids = new ArrayList<String>();
        for (int i = 2; i < 2+numMembers; i++)
        {
            var url = addMember(groupUrl, i);
            var id = url.substring(url.lastIndexOf('/')+1);
            ids.add(id);
        }
        
        // get list of members
        var jsonResp = sendGetRequest(concat(groupUrl, MEMBER_COLLECTION));
        System.out.println(gson.toJson(jsonResp));
        checkCollectionItemIds(ids, jsonResp);
    }
    
    
    protected String addMember(String parentUrl, int num) throws Exception
    {
        // add group member
        var json = createSystemGeoJson(num);
        var httpResp = sendPostRequest(concat(parentUrl, MEMBER_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequest(url);
        //System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertSystemEquals(json, (JsonObject)jsonResp);
        
        return url;
    }
    

    /*------*/
    /* FOIs */
    /*------*/
    
    @Test
    public void testAddSystemAndFois() throws Exception
    {
        // add system
        var procUrl = addSystem(10);
        
        // add foi
        int numFois = 10;
        var ids = new ArrayList<String>();
        for (int i = 0; i < numFois; i++)
        {
            var url = addFoi(procUrl, i);
            var id = url.substring(url.lastIndexOf('/')+1);
            ids.add(id);
        }
        
        // get list of fois
        var jsonResp = sendGetRequest(concat(procUrl, FOI_COLLECTION));
        System.out.println(gson.toJson(jsonResp));
        checkCollectionItemIds(ids, jsonResp);
    }
    
    
    protected String addFoi(String procUrl, int num) throws Exception
    {
        // add foi
        var json = createFoiGeoJson(num);
        var httpResp = sendPostRequest(concat(procUrl, FOI_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequest(url);
        //System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        assertDatastreamEquals(json, (JsonObject)jsonResp);
        
        return url;
    }
    
    
    protected JsonObject createFoiGeoJson(int procNum) throws Exception
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
                .add("uid", String.format("urn:osh:foi:test%03d", procNum))
                .add("name", "Sampling Feature #" + procNum)
                .add("description", "Sensor Station #" + procNum)
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
    
    
    /*-------------*/
    /* Datastreams */
    /*-------------*/
    
    @Test
    public void testAddSystemAndDatastreams() throws Exception
    {
        // add system
        var procUrl = addSystem(1);
        
        // add datastreams
        int numDatastreams = 10;
        var ids = new ArrayList<String>();
        for (int i = 0; i < numDatastreams; i++)
        {
            var url = addDatastream(procUrl, i);
            var id = url.substring(url.lastIndexOf('/')+1);
            ids.add(id);
        }
        
        // get list of datastreams
        var jsonResp = sendGetRequest(concat(procUrl, DATASTREAM_COLLECTION));
        System.out.println(gson.toJson(jsonResp));
        checkCollectionItemIds(ids, jsonResp);
    }
    
    
    protected String addDatastream(String procUrl, int num) throws Exception
    {
        var swe = new SWEHelper();
        var rec = swe.createRecord()
            .name(String.format("output%03d", num))
            .addSamplingTimeIsoUTC("time")
            .addField("f1", swe.createQuantity()
                .label("Component 1")
                .uomCode("Cel"))
            .addField("f2", swe.createText()
                .label("Component 2")
                .pattern("[0-9]{5-10}"))
            .build();
        
        // add datastream
        var json = createDatastream(rec, new TextEncodingImpl());
        var httpResp = sendPostRequest(concat(procUrl, DATASTREAM_COLLECTION), json);
        var url = getLocation(httpResp);
        
        // get it back by id
        var jsonResp = sendGetRequest(url);
        //System.out.println(gson.toJson(jsonResp));
        checkId(url, jsonResp);
        //assertDatastreamEquals(json, (JsonObject)jsonResp);
        
        return url;
    }
    
    
    protected JsonObject createDatastream(DataComponent resultStruct, DataEncoding resultEncoding) throws Exception
    {
        var buffer = new StringWriter();
        var writer = new JsonWriter(buffer);
        
        writer.beginObject();
        writer.name("name").value(resultStruct.getName());
        
        // result schema & encoding
        try
        {
            SWEJsonStreamWriter sweWriter = new SWEJsonStreamWriter(writer);
            SWEStaxBindings sweBindings = new SWEStaxBindings();
            
            writer.name("resultSchema");
            sweBindings.writeDataComponent(sweWriter, resultStruct, false);
            
            sweWriter.resetContext();
            writer.name("resultEncoding");
            sweBindings.writeAbstractEncoding(sweWriter, resultEncoding);
        }
        catch (Exception e)
        {
            throw new IOException("Error writing SWE Common result structure", e);
        }
        
        writer.endObject();
        
        return (JsonObject)JsonParser.parseString(buffer.getBuffer().toString());
    }
    
    
    protected void assertDatastreamEquals(JsonObject expected, JsonObject actual)
    {
        // remove some fields not present in POST request before comparison
        actual.remove("id");
        actual.remove("system");
        actual.remove("validTime");
        actual.remove("phenomenonTime");
        actual.remove("resultTime");
        actual.remove("links");
        assertEquals(expected, actual);
    }
    
    
    /*--------------*/
    /* Observations */
    /*--------------*/
    
    @Test
    public void testAddDatastreamAndObservations() throws Exception
    {
        // add system
        var procUrl = addSystem(33);
        
        // add datastream
        var dsUrl = addDatastream(procUrl, 115);
        
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
        
        // get list of datastreams
        var jsonResp = sendGetRequest(concat(dsUrl, OBS_COLLECTION));
        System.out.println(gson.toJson(jsonResp));
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
        var jsonResp = sendGetRequest(url);
        //var jsonResp = sendGetRequest(concat(dsUrl, OBS_COLLECTION));
        System.out.println(gson.toJson(jsonResp));
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
                .name("f2").value(String.format("text_%03d", num))
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
        actual.remove("datastreamId");
        assertEquals(expected, actual);
    }
    
        
    /*-----------------*/
    /* Utility methods */
    /*-----------------*/
        
    protected String checkId(String url, JsonElement actual)
    {
        assertTrue(actual instanceof JsonObject);
        var id = ((JsonObject)actual).get("id").getAsString();
        assertEquals(url.substring(url.lastIndexOf('/')+1), id);
        return id;
    }
    
    
    protected void checkCollectionItemIds(Collection<String> expectedIds, JsonElement collection)
    {
        var collectionItems = ((JsonObject)collection).get("items").getAsJsonArray();
        var collectionSize = collectionItems.size();
        assertEquals(expectedIds.size(), collectionSize);
        
        var collectionsIds = new HashSet<>();
        for (int i = 0; i < collectionSize; i++)
        {
            var member = (JsonObject)collectionItems.get(i);
            collectionsIds.add(member.get("id").getAsString());
        }
        
        for (var id: expectedIds)
            assertTrue("Resource with id " + id + " missing from collection", collectionsIds.contains(id));
    }
    
    
    protected HttpResponse<String> sendPostRequest(String path, JsonElement json) throws IOException
    {
        return sendPostRequest(path, json, ResourceFormat.JSON.getMimeType());
    }
    
    
    protected HttpResponse<String> sendPostRequest(String path, JsonElement json, String contentType) throws IOException
    {
        try
        {
            HttpClient client = HttpClient.newHttpClient();
            
            var jsonString = gson.toJson(json);
            HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(jsonString))
                .uri(URI.create(concat(swaRootUrl, path)))
                .header("Content-Type", contentType)
                .build();
            
            log.info("Sending " + request + "\n" + jsonString);
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            
            // print error and send exception
            if (statusCode >= 300)
            {
                System.err.println(response.body());
                throw new IOException("Received HTTP error code " + statusCode);
            }
            
            return response;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }
    
    
    protected JsonElement sendGetRequest(String path) throws IOException
    {
        try
        {
            HttpClient client = HttpClient.newHttpClient();
            
            HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(concat(swaRootUrl, path)))
                .build();
            
            log.info("Sending " + request);
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            
            // print error and send exception
            if (statusCode >= 300)
            {
                System.err.println(response.body());
                throw new IOException("Received HTTP error code " + statusCode);
            }
            
            return JsonParser.parseString(response.body());
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }
    
    
    protected String concat(String url, String path)
    {
        if (!url.endsWith("/") && !path.startsWith("/"))
            url += "/";
        else if (url.endsWith("/") && path.startsWith("/"))
            url = url.substring(0, url.length()-1);
        return url + path;
    }
    
    
    protected String getLocation(HttpResponse<?> httpResp)
    {
        var url = httpResp.headers().firstValue("Location").orElse(null);
        assertNotNull(url);
        return url;
    }
    
    
    @After
    public void cleanup()
    {
        try
        {
            if (hub != null)
                hub.stop();
        }
        finally
        {
            if (dbFile != null)
                dbFile.delete();
        }
    }

}
