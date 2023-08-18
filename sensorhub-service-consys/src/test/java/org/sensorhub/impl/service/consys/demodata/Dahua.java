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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import org.isotc211.v2005.gmd.CIResponsibleParty;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLMetadataBuilders.CIResponsiblePartyBuilder;
import org.vast.sensorML.helper.CommonCharacteristics;
import org.vast.sensorML.helper.CommonClassifiers;
import org.vast.sensorML.helper.CommonIdentifiers;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import org.vast.swe.helper.GeoPosHelper;
import org.vast.swe.helper.RasterHelper;
import net.opengis.gml.v32.Point;
import net.opengis.gml.v32.impl.GMLFactory;
import net.opengis.sensorml.v20.AbstractProcess;


public class Dahua
{
    public static final String SD22404_PROC_UID = "urn:x-dahua:cam:sd22204t-gn";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    static AbstractProcess createSD22204Datasheet()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID(SD22404_PROC_UID)
            .name("Dahua PTZ Camera SD22204T-GN")
            .description("The camera features powerful optical zoom and accurate pan/tilt/zoom performance, "
                + "providing a wide monitoring range and great detail. It delivers 2MP resolution at 25/30fps. "
                + "It is equipped with smooth control, high quality image, and good protection, meeting compact "
                + "size demands of video surveillance applications.")
            
            .addIdentifier(sml.identifiers.shortName("Dahua SD22204T-GN"))
            .addIdentifier(sml.identifiers.longName("Dahua PTZ Camera SD22204T-GN"))
            .addIdentifier(sml.identifiers.manufacturer("Dahua Technology"))
            .addIdentifier(sml.identifiers.modelNumber("SD22204T-GN"))
            .addClassifier(sml.classifiers.sensorType("Camera"))
            
            .addInput("reflectivity", sml.createObservableProperty()
                .definition("https://mmisw.org/ont/ioos/parameter/echo_intensity")
                .label("Reflectivity")
                .build()
            )
            
            .validFrom(OffsetDateTime.parse("2018-09-05T00:00:00Z"))
            
            .addCharacteristicList("mech_specs", sml.createCharacteristicList()
                .label("Mechanical Characteristics")
                .add("weight", sml.characteristics.mass(710, "g"))
                .add("diameter", sml.characteristics.diameter(122, "mm"))
                .add("height", sml.characteristics.height(89, "mm"))
                .add("material", sml.characteristics.material("Metal")
                    .label("Casing Material"))
            )
            
            .addCharacteristicList("elec_specs", sml.createCharacteristicList()
                .label("Electrical Characteristics")
                .add("voltage", sml.characteristics.operatingVoltage(12, "V")
                    .label("Input Voltage (DC)"))
                .add("current", sml.characteristics.operatingCurrent(1.5, "A")
                    .label("Max Current"))
                .add("if_type", sml.createText()
                    .label("Interface Type")
                    .value("Ethernet 100Base-T (RJ-45 with PoE)"))
            )
            
            .addCharacteristicList("sensor", sml.createCharacteristicList()
                .label("Image Sensor Characteristics")
                .add("detector_type", sml.createCategory()
                    .definition(CommonClassifiers.SENSOR_TYPE_DEF)
                    .label("Detector Type")
                    .value("CMOS")
                )
                .add("detector_size", sml.createQuantity()
                    .definition(CommonCharacteristics.WIDTH_DEF)
                    .label("Detector Size")
                    .description("1/3\"")
                    .uom("[in_i]")
                    .value(0.3333)
                )
                .add("pixel_width", sml.createCount()
                    .definition(RasterHelper.DEF_RASTER_WIDTH)
                    .label("Pixel Width")
                    .value(1920)
                )
                .add("pixel_height", sml.createCount()
                    .definition(RasterHelper.DEF_RASTER_HEIGHT)
                    .label("Pixel Height")
                    .value(1080)
                )
                .add("video_formats", sml.createText()
                    .definition(SWEHelper.getPropertyUri("SupportedFormats"))
                    .label("Video Formats")
                    .value("H.264, MJPEG")
                )
                .add("bitrate_h264", sml.createQuantityRange()
                    .definition(SWEHelper.getDBpediaUri("Bit_rate"))
                    .label("Video Bitrate (H264)")
                    .uom("kbit/s")
                    .value(448, 8192)
                )
                .add("bitrate_mjpeg", sml.createQuantityRange()
                    .definition(SWEHelper.getDBpediaUri("Bit_rate"))
                    .label("Max Video Bitrate (MJPEG)")
                    .uom("kbit/s")
                    .value(5120, 10240)
                )
            )
            
            .addCapabilityList("pointing_specs", sml.capabilities.systemCapabilities()
                .label("Pointing Capabilities")
                .add("pan_range", sml.capabilities.pointingRange(0, 360, "deg")
                    .label("Pan Range"))
                .add("elev_range", sml.capabilities.pointingRange(0, 90, "deg")
                    .label("Tilt Range"))
                .add("pointing_Error", sml.capabilities.absoluteAccuracy(0.5, "deg")
                    .label("Pointing Error"))
                .add("pan_rate", sml.createQuantity()
                    .definition(GeoPosHelper.DEF_ANGULAR_VELOCITY)
                    .label("Max Pan Rate")
                    .uom("deg/s")
                    .value(100.0)
                )
                .add("tiltrate", sml.createQuantity()
                    .definition(GeoPosHelper.DEF_ANGULAR_VELOCITY)
                    .label("Max Tilt Rate")
                    .uom("deg/s")
                    .value(60.0)
                )
            )
            
            .addCapabilityList("sys_caps", sml.capabilities.systemCapabilities()
                .label("Sensor Capabilities")
                .add("frame_rate", sml.capabilities.samplingFrequency(30))
                .add("shutter_speed", sml.createQuantityRange()
                    .definition(SWEHelper.getDBpediaUri("Shutter_speed"))
                    .label("Shutter Speed")
                    .uom("ms")
                    .value(0.03, 1000)
                )
                .add("resolution", sml.capabilities.resolution(8, "bit"))
                .add("snr", sml.capabilities.snr(50))
                .add("focal", sml.capabilities.focalLengthRange(2.7, 11, "mm"))
                .add("fov", sml.capabilities.fovRange(30, 112.5, "deg"))
             )
            
            .addCapabilityList("op_range", sml.capabilities.operatingProperties()
                .add("temperature", sml.conditions.temperatureRange(-30, 60, "Cel"))
                .add("ingress", sml.createCategory()
                    .definition(SWEHelper.getDBpediaUri("IP_code"))
                    .label("Ingress Protection")
                    .value("IP66"))
            )
            
            .addContact(getDahuaContactInfo()
                .role(CommonIdentifiers.MANUFACTURER_DEF)
            )
            
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, sml.createDocument()
                .name("Product Webpage")
                .description("Product webpage on manufacturer's website")
                .url("https://www.dahuasecurity.com/products/All-Products/Discontinued-Products/PTZ-Cameras/Lite-Series/SD22204T-GN-S2=S2")
                .mediaType("text/html")
            )
            .addDocument(CommonIdentifiers.SPECSHEET_DEF, sml.createDocument()
                .name("Datasheet")
                .url("https://www.dahuasecurity.com/asset/upload/product/20180905/SD22204T-GN_Datasheet_20180905.pdf")
                .mediaType("application/pdf")
            )
            .build();
    }
    
    
    static AbstractProcess createCameraInstance(String id, Point location, double heading, Instant startTime)
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID("urn:x-safecity:sg:dahua:" + id)
            .name("Dahua PTZ Cam " + id)
            .typeOf(SD22404_PROC_UID)
            .addIdentifier(sml.identifiers.shortName("Dahua PTZ Cam " + id))
            .addContact(getOperatorContactInfo())
            .validFrom(startTime.atOffset(ZoneOffset.UTC))
            .location(location)
            .build();
    }
    
    
    static Collection<AbstractProcess> getAllCameras()
    {
        var list = new ArrayList<AbstractProcess>(100);
        
        var fac = new GMLFactory();
        var locations = new Point[] {
            fac.newPoint(103.868811, 1.327909),
            fac.newPoint(103.879014, 1.331288),
            fac.newPoint(103.869337, 1.332328),
            fac.newPoint(103.863479, 1.322020),
            fac.newPoint(103.862309, 1.330440),
            fac.newPoint(103.855743, 1.329089)
        };
        
        for (int i = 0; i < locations.length; i++)
        {
            list.add(createCameraInstance(
                String.format("SG%05d", (i+10)),
                locations[i],
                0.0,
                Instant.parse("2020-04-28T08:00:00Z").plus((i+1)*(i-1), ChronoUnit.DAYS)
            ));
        }
        
        return list;
    }
    
    
    static CIResponsiblePartyBuilder getDahuaContactInfo()
    {
        return sml.createContact()
            .organisationName("Dahua Technology Co., Ltd.")
            .website("https://www.dahuasecurity.com")
            .deliveryPoint("No.1199, Bin'an Road, Binjiang District")
            .city("Hangzhou")
            .postalCode("310053")
            .country("China")
            .phone("+86 571 8768 8815")
            .email("overseas@dahuatech.com");
    }
    
    
    static CIResponsibleParty getOperatorContactInfo()
    {
        return sml.createContact()
            .role(CommonIdentifiers.OPERATOR_DEF)
            .organisationName("Safe City Inc.")
            .website("https://www.safecity.com")
            .country("Singapore")
            .build();
    }

}
