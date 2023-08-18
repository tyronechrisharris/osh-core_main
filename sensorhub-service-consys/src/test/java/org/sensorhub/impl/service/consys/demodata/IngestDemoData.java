/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2023 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.demodata;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Base64;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLJsonBindings;
import org.vast.swe.helper.GeoPosHelper;
import com.google.common.net.HttpHeaders;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.Deployment;
import net.opengis.sensorml.v20.DescribedObject;


public class IngestDemoData
{
    static String API_ROOT = "http://localhost:8181/sensorhub/api/";
    static String CREDENTIALS = "admin:test";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    public static void main(String[] args) throws IOException
    {
        // Mavic UAV
        addOrUpdateProcedure(MavicPro.createPlatformDatasheet(), true);
        addOrUpdateProcedure(MavicPro.createCameraDatasheet(), true);
        addOrUpdateProcedure(MavicPro.createInsGpsDatasheet(), true);
        
        addOrUpdateSystem(MavicPro.createPlatformInstance("08QDE5J01200B3", Instant.parse("2017-08-24T12:00:00Z")), true);
        addOrUpdateSystem(MavicPro.createPlatformInstance("09FGY7C56897F4", Instant.parse("2018-04-11T18:00:00Z")), true);
        
        // Saildrone USV
        addOrUpdateProcedure(Saildrone.createPlatformDatasheet(), true);
        addOrUpdateSystem(Saildrone.createPlatformInstance("1001", Instant.parse("2017-08-24T12:00:00Z")), true);
        
        addOrUpdateProcedure(VectorNav.createVN200Datasheet(), true);
        addOrUpdateProcedure(Rotronic.createHC2Datasheet(), true);
        addOrUpdateProcedure(Vaisala.createPTB210Datasheet(), true);
        addOrUpdateProcedure(Gill.createWindmasterDatasheet(), true);
        addOrUpdateProcedure(Aanderaa.createOX4831Datasheet(), true);
        addOrUpdateProcedure(Seabird.createSBE37Datasheet(), true);
        
        // Davis
        addOrUpdateProcedure(Davis.createVantagePro2Datasheet(), true);
        for (var sys: Davis.getAllStations())
            addOrUpdateSystem(sys, true);
        
        // Nexrad
        addOrUpdateProcedure(Nexrad.createWSR88DDatasheet(), true);
        for (var sys: Nexrad.getAllRadarSites())
            addOrUpdateSystem(sys, true);
        
        // GFS
        addOrUpdateProcedure(GfsModel.createGFSModelSpecs(), true);
        addOrUpdateSystem(GfsModel.createModelInstance(Instant.parse("2022-11-29T00:00:00Z")), true);
        
        // Humans
        addOrUpdateProcedure(Humans.createBirdSurveyProcedure(), true);
        for (var sys: Humans.getAllBirdWatchers())
            addOrUpdateSystem(sys, true);
        addOrUpdateProcedure(Humans.createWaterSamplingProcedure(), true);
        
        // Dahua
        addOrUpdateProcedure(Dahua.createSD22204Datasheet(), true);
        for (var sys: Dahua.getAllCameras())
            addOrUpdateSystem(sys, true);
    }
    
    
    static String addOrUpdateProcedure(AbstractProcess obj, boolean replace) throws IOException
    {
        return addOrUpdateResource("procedures", obj, replace);
    }
    
    
    static String addOrUpdateSystem(AbstractProcess obj, boolean replace) throws IOException
    {
        return addOrUpdateResource("systems", obj, replace);
    }
    
    
    static String addOrUpdateSubsystem(String parentUid, AbstractProcess obj, boolean replace) throws IOException
    {
        var id = getFeatureByUid("systems", parentUid);
        if (id == null)
            throw new IllegalArgumentException("Parent system not found: " + parentUid);
        return addOrUpdateResource("systems/" + id + "/members", obj, replace);
    }
    
    
    static String addOrUpdateDeployment(Deployment obj, boolean replace) throws IOException
    {
        return addOrUpdateResource("deployments", obj, replace);
    }
    
    
    static String addOrUpdateResource(String resourceType, DescribedObject obj, boolean replace) throws IOException
    {
        // check if procedure exists and retrieve ID
        var id = getFeatureByUid(resourceType, obj.getUniqueIdentifier());
        
        var strWriter = new StringWriter();
        var bindings = new SMLJsonBindings();
        bindings.writeDescribedObject(new JsonWriter(strWriter), obj);
        
        if (id == null || !replace)
        {
            var resp = sendPostRequest("/" + resourceType, strWriter.toString(), "application/sml+json");
            id = resp.headers().firstValue(HttpHeaders.LOCATION).get();
        }
        else
            sendPutRequest("/" + resourceType + "/" + id, strWriter.toString(), "application/sml+json");
        
        return id;
    }
    
    
    static String getFeatureByUid(String path, String uid) throws IOException
    {
        HttpClient client = HttpClient.newHttpClient();
        
        try
        {
            HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(API_ROOT + "/" + path + "?select=id&uid=" + uid))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(CREDENTIALS.getBytes()))
                .build();
            
            System.out.println(request);
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            int statusCode = response.statusCode();
            
            // print error and send exception
            if (statusCode >= 300)
            {
                System.err.println(response.body());
                throw new IOException("Received HTTP error code " + statusCode);
            }
            
            // return resource ID if response is non-empty
            var json = JsonParser.parseReader(new StringReader(response.body()));
            var items = json.getAsJsonObject().get("items").getAsJsonArray();
            if (!items.isEmpty())
                return items.get(0).getAsJsonObject().get("id").getAsString();
            
            return null;
        }
        catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }
    
    
    static HttpResponse<String> sendPostRequest(String path, String body, String contentType) throws IOException
    {
        try
        {
            HttpClient client = HttpClient.newHttpClient();
            
            HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .uri(URI.create(API_ROOT + path))
                .header("Content-Type", contentType)
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(CREDENTIALS.getBytes()))
                .build();
            
            System.out.println(request);// + "\n" + jsonString);
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
    
    
    static HttpResponse<String> sendPutRequest(String path, String body, String contentType) throws IOException
    {
        try
        {
            HttpClient client = HttpClient.newHttpClient();
            
            HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofString(body))
                .uri(URI.create(API_ROOT + path))
                .header("Content-Type", contentType)
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString(CREDENTIALS.getBytes()))
                .build();
            
            System.out.println(request);// + "\n" + jsonString);
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

}
