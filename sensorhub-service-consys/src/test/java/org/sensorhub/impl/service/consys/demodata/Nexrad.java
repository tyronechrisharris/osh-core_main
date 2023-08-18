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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import org.isotc211.v2005.gmd.CIResponsibleParty;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLMetadataBuilders.CIResponsiblePartyBuilder;
import org.vast.sensorML.helper.CommonIdentifiers;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import org.vast.swe.helper.GeoPosHelper;
import net.opengis.gml.v32.Point;
import net.opengis.gml.v32.impl.GMLFactory;
import net.opengis.sensorml.v20.AbstractProcess;


public class Nexrad
{
    public static final String WSR88D_PROC_UID = "urn:x-noaa:sensor:wsr88d";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    static AbstractProcess createWSR88DDatasheet()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID(WSR88D_PROC_UID)
            .name("NEXRAD Weather Surveillance Radar (WSR-88D)")
            .description("The NEXRAD radar detects precipitation and atmospheric movement or wind."
                + "It incorporates a number of improvements over the radar systems that were "
                + "previously in use. The new system provided Doppler velocity, improving tornado prediction ability "
                + "by detecting rotation present within the storm at different scan angles. It provided improved "
                + "resolution and sensitivity, enabling operators to see features such as cold fronts, thunderstorm "
                + "gust fronts, and mesoscale to even storm scale features of thunderstorms that had never been "
                + "visible on radar. The NEXRAD radars also provided volumetric scans of the atmosphere allowing "
                + "operators to examine the vertical structure of storms and could act as wind profilers by providing "
                + "detailed wind information for several kilometers above the radar site. The radars also had a much "
                + "increased range allowing detection of weather events at much greater distances from the radar site.")
            
            .addIdentifier(sml.identifiers.shortName("NEXRAD Radar"))
            .addIdentifier(sml.identifiers.longName("NEXRAD Weather Surveillance Radar (WSR-88D)"))
            .addIdentifier(sml.identifiers.manufacturer("Unisys Corporation"))
            .addIdentifier(sml.identifiers.modelNumber("WSR-88D"))
            .addClassifier(sml.classifiers.sensorType("Radar"))
            
            .addInput("reflectivity", sml.createObservableProperty()
                .definition("https://mmisw.org/ont/ioos/parameter/echo_intensity")
                .label("Reflectivity")
                .build()
            )
            .addInput("velocity", sml.createObservableProperty()
                .definition("https://mmisw.org/ont/ioos/parameter/radial_velocity")
                .label("Radial Velocity")
                .build()
            )
            
            .validFrom(OffsetDateTime.parse("1988-01-01T00:00:00Z"))
            
            .addCharacteristicList("antenna", sml.createCharacteristicList()
                .label("Antenna Characteristics")
                .add("radome_diam", sml.characteristics.diameter(11.89, "m")
                    .label("Radome Diameter"))
                .add("dish_diam", sml.characteristics.diameter(9.1, "m")
                    .label("Dish Diameter"))
            )
            
            .addCharacteristicList("transmitter", sml.createCharacteristicList()
                .label("Transmitter Characteristics")
                .add("freq_band", sml.createCategory()
                    .definition("http://sensorml.com/ont/swe/spectrum/FrequencyBand")
                    .codeSpace("http://sensorml.com/ont/swe/spectrum/IEEE")
                    .label("Frequency Band")
                    .value("S_band"))
                .add("freq_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Frequency"))
                    .label("Frequency Range")
                    .uom("MHz")
                    .value(2700.0, 3000.0))
                .add("power", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("RF-Power"))
                    .label("Transmitted Power")
                    .uom("W")
                    .value(300, 1500))
                .add("gain", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Gain"))
                    .label("Gain")
                    .uom("dB")
                    .value(53))
                .add("pulse", sml.createQuantityRange()
                    .definition("http://dbpedia.org/resource/Pulse_width")
                    .label("Pulse Width")
                    .uom("us")
                    .value(1.57, 4.55))
            )
            
            .addCharacteristicList("receiver", sml.createCharacteristicList()
                .label("Receiver Characteristics")
                .add("interm_freq", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Frequency"))
                    .label("Intermediate Frequency")
                    .uom("MHz")
                    .value(57.55))
                .add("noise", sml.createQuantity()
                    .definition("http://dbpedia.org/resource/Noise_figure")
                    .label("Noise Figure")
                    .uom("dB")
                    .value(2.7))
            )
            
            .addCapabilityList("pointing_specs", sml.capabilities.systemCapabilities()
                .label("Pointing Capabilities")
                .add("azim_range", sml.capabilities.pointingRange(0, 360, "deg")
                    .label("Azimuth Range"))
                .add("elev_range", sml.capabilities.pointingRange(-1, 45, "deg")
                    .label("Elevation Range"))
                .add("pointing_Error", sml.capabilities.absoluteAccuracy(0.2, "deg")
                    .label("Pointing Error"))
                .add("max_rate", sml.createQuantity()
                    .definition(GeoPosHelper.DEF_ANGULAR_VELOCITY)
                    .label("Max Rotation Rate")
                    .uom("deg/s")
                    .value(30.0)
                )
                .add("max_accel", sml.createQuantity()
                    .definition(GeoPosHelper.DEF_ANGULAR_ACCEL)
                    .label("Max Rotation Acceleration")
                    .uom("deg/s2")
                    .value(30.0)
                )
            )
            
            .addCapabilityList("refl_caps", sml.capabilities.systemCapabilities()
                .label("Reflectivity Measurement Capabilities (dBZ)")
                .add("range", sml.capabilities.measurementRange(1, 95, "dB"))
                .add("resolution", sml.capabilities.resolution(0.1, "dB"))
                .add("accuracy", sml.capabilities.absoluteAccuracy(0.5, "dB")
                    .label("Absolute Accuracy (1σ)"))
                .add("dist_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Measurement Distance")
                    .uomCode("km")
                    .value(0, 460)
                )
                .add("range_resolution", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Range Resolution")
                    .uomCode("km")
                    .value(1.0)
                )
                .add("az_resolution", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Angle"))
                    .label("Azimuth Resolution")
                    .uomCode("deg")
                    .value(1.0)
                )
            )
            
            .addCapabilityList("vel_caps", sml.capabilities.systemCapabilities()
                .label("Velocity Measurement Capabilities")
                .add("range", sml.capabilities.measurementRange(-32, 32, "m/s"))
                .add("resolution", sml.capabilities.resolution(0.1, "m/s"))
                .add("accuracy", sml.capabilities.absoluteAccuracy(0.5, "m/s")
                    .label("Absolute Accuracy (1σ)"))
                .add("dist_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Measurement Distance")
                    .uomCode("km")
                    .value(0, 230)
                )
                .add("range_resolution", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Range Resolution")
                    .uomCode("km")
                    .value(0.25)
                )
                .add("az_resolution", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Angle"))
                    .label("Azimuth Resolution")
                    .uomCode("deg")
                    .value(1.0)
                )
            )
            
            .addCapabilityList("op_range", sml.capabilities.operatingProperties()
                .add("temperature", sml.conditions.temperatureRange(-40, 60, "Cel"))
            )
            
            .addContact(getUnisysContactInfo()
                .role(CommonIdentifiers.MANUFACTURER_DEF)
            )
            
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, sml.createDocument()
                .name("NEXRAD NOAA Page")
                .description("Webpage with technical information")
                .url("https://www.roc.noaa.gov/WSR88D/Engineering/NEXRADTechInfo.aspx")
                .mediaType("text/html")
            )
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, sml.createDocument()
                .name("NEXRAD Wikipedia Page")
                .url("https://en.wikipedia.org/wiki/NEXRAD")
                .mediaType("text/html")
            )
            .build();
    }
    
    
    static AbstractProcess createNexradInstance(String ncdcCode, String icaoCode, String siteName, Point location, Instant startTime)
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID("urn:x-noaa:nexrad:" + icaoCode)
            .name("NEXRAD Weather Radar " + icaoCode)
            .description("NEXRAD weather radar " + icaoCode + " located at " + siteName)
            .typeOf(WSR88D_PROC_UID)
            .addIdentifier(sml.identifiers.shortName("NEXRAD " + icaoCode))
            .addIdentifier(sml.identifiers.siteId(icaoCode, "ICAO")
                .label("Site ID (ICAO)"))
            .addIdentifier(sml.identifiers.siteId(ncdcCode, "NCDC")
                .label("Site ID (NCDC)"))
            .addContact(getNoaaRocContactInfo())
            .validFrom(startTime.atOffset(ZoneOffset.UTC))
            .location(location)
            .build();
    }
    
    
    static Collection<AbstractProcess> getAllRadarSites()
    {
        var radarList = new ArrayList<AbstractProcess>(100);
        
        var nexradSitesFile = IngestDemoData.class.getResourceAsStream("NexradLocations.txt");
        try (var reader = new BufferedReader(new InputStreamReader(nexradSitesFile)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                var ncdcId = line.substring(0,8);
                var icaoId = line.substring(9,13);
                var siteName = line.substring(20, 51).trim();
                var country = line.substring(51, 72).trim();
                var stateCode = line.substring(72, 74);
                var county = line.substring(75, 106).trim();
                var lat = line.substring(106, 116).trim();
                var lon = line.substring(116, 127).trim();
                var alt = line.substring(127, 134).trim();
                
                if ("UNITED STATES".equalsIgnoreCase(country))
                {
                    var fullName = siteName + ", " + stateCode + " (" + county + ")";
                    var location = new GMLFactory().newPoint(
                        Double.parseDouble(lon),
                        Double.parseDouble(lat),
                        Double.parseDouble(alt) * 0.3048
                    );
                    location.setSrsName(SWEConstants.REF_FRAME_CRS84h);
                    location.setSrsDimension(3);
                    
                    var startValidTime = Instant.parse("1992-01-01T00:00:00Z"); 
                    
                    radarList.add(Nexrad.createNexradInstance(
                        ncdcId, icaoId, fullName, location, startValidTime));
                }
            }
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
        
        return radarList;
    }
    
    
    static CIResponsiblePartyBuilder getUnisysContactInfo()
    {
        return sml.createContact()
            .organisationName(" Unisys Corporation")
            .website("https://www.unisys.com")
            .deliveryPoint("801 Lakeview Drive, Ste 100")
            .city("Blue Bell")
            .postalCode("19422")
            .administrativeArea("PA")
            .country("USA")
            .phone("+1 800 874-8647")
            .email("info@unisys.com");
    }
    
    
    static CIResponsibleParty getNoaaRocContactInfo()
    {
        return sml.createContact()
            .role(CommonIdentifiers.OPERATOR_DEF)
            .organisationName("NOAA/NWS Radar Operations Center")
            .website("https://www.roc.noaa.gov")
            .deliveryPoint("1200 Westheimer Drive")
            .city("Norman")
            .postalCode("73069")
            .administrativeArea("OK")
            .country("USA")
            .phone("+1 405-573-8815")
            .build();
    }

}
