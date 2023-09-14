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
import java.time.ZoneOffset;
import org.isotc211.v2005.gmd.CIOnlineResource;
import org.isotc211.v2005.gmd.CIResponsibleParty;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.helper.CommonCapabilities;
import org.vast.sensorML.helper.CommonCharacteristics;
import org.vast.sensorML.helper.CommonClassifiers;
import org.vast.sensorML.helper.CommonIdentifiers;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import org.vast.swe.helper.GeoPosHelper;
import org.vast.swe.helper.RasterHelper;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.CapabilityList;


public class MavicPro
{
    static final String MAVICPRO_PROC_UID = "urn:x-dji:platform:mavicpro";
    static final String CAMERA_PROC_UID = "urn:x-dji:sensor:mavicpro:camera";
    static final String INSGPS_PROC_UID = "urn:x-dji:sensor:mavicpro:insgps";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    static AbstractProcess createPlatformDatasheet()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID(MAVICPRO_PROC_UID)
            .name("DJI Mavic Pro")
            .description("The Mavic Pro was the first of the Mavic series, released in late 2016. "
                + "The drone is capable of capturing 4K video, has a flight range of 6.9 km (4.3 miles) "
                + "and a flight time of 24 minutes. Top speed is 65 km/h (40 mph) in sport mode. The "
                + "brand-new Ocusync transmission system can livestream video at a distance of 7 km "
                + "(4.3 miles) in 1080p.")
            
            .addIdentifier(sml.identifiers.shortName("DJI Mavic Pro"))
            .addIdentifier(sml.identifiers.modelNumber("Mavic Pro"))
            
            .addCharacteristicList("physical", sml.createCharacteristicList()
                .label("Physical Characteristics")
                .add("weight", sml.characteristics.mass(734, "g"))
                .add("length0", sml.characteristics.length(198, "mm")
                    .label("Length (folded)"))
                .add("width0", sml.characteristics.width(83, "mm")
                    .label("Width (folded)"))
                .add("height0", sml.characteristics.height(83, "mm")
                    .label("Height (folded)"))
                .add("length", sml.characteristics.length(335, "mm")
                    .label("Length (unfolded)"))
                .add("width", sml.characteristics.width(335, "mm")
                    .label("Width (unfolded)"))
                .add("height", sml.characteristics.height(83, "mm")
                    .label("Height (unfolded)"))
             )
            .addCharacteristicList("battery", sml.createCharacteristicList()
                .label("Battery Characteristics")
                .add("bat_cap", sml.characteristics.batteryCapacity(43.6, "W.h"))
                .add("bat_volt", sml.characteristics.operatingVoltage(11.4, "V"))
                .add("type", sml.createCategory()
                    .label("Battery Type")
                    .value("LiPo 3S"))
             )
            .addCharacteristicList("radio", sml.createCharacteristicList()
                .label("Radio Characteristics")
                .add("frequency_band", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Frequency"))
                    .label("Frequency Band")
                    .uom("GHz")
                    .value(2.4, 2.483)
                )
                .add("power_fcc", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("RF-Power"))
                    .label("Transmitter Power (EIRP, FCC)")
                    .uom("dB[mW]")
                    .value(26)
                )
                .add("power_ce", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("RF-Power"))
                    .label("Transmitter Power (EIRP, CE)")
                    .uom("dB[mW]")
                    .value(20)
                )
                .add("power_srrc", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("RF-Power"))
                    .label("Transmitter Power (EIRP, SRRC)")
                    .uom("dB[mW]")
                    .value(20)
                )
                .add("radio_range_fcc", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Max Radio Range (FCC)")
                    .uom("km")
                    .value(7)
                )
                .add("radio_range_ce", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Max Radio Range (CE)")
                    .uom("km")
                    .value(4)
                )
                .add("radio_range_srrc", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Max Radio Range (SRRC)")
                    .uom("km")
                    .value(4)
                )
             )
            .addCapabilityList("nav_capabilities", sml.capabilities.systemCapabilities()
                .addCondition("wind_speed", sml.createQuantity()
                    .definition(SWEHelper.getCfUri("wind_speed"))
                    .label("Wind Speed")
                    .uomCode("m/s")
                    .value(0)
                )
                .add("flight_range", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Distance"))
                    .label("Max Travel Distance")
                    .uomCode("km")
                    .value(13.0)
                )
                .add("max_speed", sml.createQuantity()
                    .definition(SWEHelper.getQudtUri("Speed"))
                    .label("Max Speed")
                    .uomCode("km/h")
                    .value(65.0)
                )
                .add("flight_time", sml.characteristics.batteryLifetime(24.0, "min")
                    .description("Maximum flight time in stationary flight")
                )
                .add("max_height", sml.createQuantity()
                    .definition(GeoPosHelper.DEF_ALTITUDE_MSL)
                    .label("Max Height")
                    .uomCode("m")
                    .value(5000.0)
                )
             )
            .addCapabilityList("op_range", sml.capabilities.operatingProperties()
                .add("temperature", sml.conditions.temperatureRange(0, 40, "Cel"))
                .add("wind_speed", sml.createQuantity()
                    .definition(SWEHelper.getCfUri("wind_speed"))
                    .label("Max Wind Speed")
                    .uomCode("km/h")
                    .value(35)
                )
             )
            
            .addContact(getDjiContactInfo())
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, getSpecWebPage())
            .addDocument(CommonIdentifiers.USER_MANUAL_DEF, sml.createDocument()
                .name("User Manual")
                .url("https://dl.djicdn.com/downloads/mavic/20171219/Mavic%20Pro%20User%20Manual%20V2.0.pdf")
                .mediaType("application/pdf")
             )
            .build();
    }
    
    
    static AbstractProcess createCameraDatasheet()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID(CAMERA_PROC_UID)
            .name("DJI Mavic Pro Camera")
            .description("Subsystem composed of a 4K video camera mounted on a gyro-stablized gimbal, onboard Mavic Pro UAV")
            
            .addIdentifier(sml.identifiers.longName("DJI Mavic Pro 4K Camera"))
            .addClassifier(sml.classifiers.sensorType("VideoCamera"))
            
            .addCharacteristicList("sensor", sml.createCharacteristicList()
                .label("Sensor Characteristics")
                .add("detector_type", sml.createCategory()
                    .definition(CommonClassifiers.SENSOR_TYPE_DEF)
                    .label("Detector Type")
                    .value("CMOS")
                )
                .add("detector_size", sml.createQuantity()
                    .definition(CommonCharacteristics.WIDTH_DEF)
                    .label("Detector Size")
                    .description("1/2.3\"")
                    .uom("[in_i]")
                    .value(0.43)
                )
                .add("pixel_size", sml.createCount()
                    .definition(SWEHelper.getQudtUri("Count"))
                    .label("Number of Pixels")
                    .value(12350000)
                )
                .add("video_formats", sml.createText()
                    .definition(SWEHelper.getPropertyUri("SupportedFormats"))
                    .label("Video Formats")
                    .value("MP4, MOV (MPEG-4 AVC/H.264)")
                )
                .add("photo_formats", sml.createText()
                    .definition(SWEHelper.getPropertyUri("SupportedFormats"))
                    .label("Photo Formats")
                    .value("JPEG, DNG")
                )
                .add("video_bitrate", sml.createQuantity()
                    .definition(SWEHelper.getDBpediaUri("Bit_rate"))
                    .label("Max Video Bitrate")
                    .uom("Mbit/s")
                    .value(60)
                )
            )
            .addCapabilityList("sys_caps", sml.capabilities.systemCapabilities()
                .label("Sensor Capabilities")
                .add("fov", sml.capabilities.fov(78.8, "deg"))
                .add("shutter_speed", sml.createQuantityRange()
                    .definition(CommonCapabilities.INTEGRATION_TIME_DEF)
                    .label("Shutter Speed")
                    .uom("s")
                    .value(1/8000., 8)
                )
                .add("video_iso", sml.createQuantityRange()
                    .definition(CommonCapabilities.SENSITIVITY_DEF)
                    .label("ISO Range (video)")
                    .uom("1")
                    .value(100, 3200)
                )
                .add("image_iso", sml.createQuantityRange()
                    .definition(CommonCapabilities.SENSITIVITY_DEF)
                    .label("ISO Range (photo)")
                    .uom("1")
                    .value(100, 1600)
                )
                .add("pitch_range", sml.createQuantityRange()
                    .definition(GeoPosHelper.DEF_PITCH_ANGLE)
                    .label("Pitch Range")
                    .uom("deg")
                    .value(-90, 30)
                )
             )
            .addCapabilityList("op_range", sml.capabilities.operatingProperties()
                .add("temperature", sml.conditions.temperatureRange(0, 40, "Cel"))
             )
            
            .addContact(getDjiContactInfo())
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, getSpecWebPage())
            
            .addMode("C4K", sml.createMode()
                .addCapabilityList("sys_caps", getModeVideoCaps(4096, 2160, 24))
            )
            .addMode("4K", sml.createMode()
                .addCapabilityList("sys_caps", getModeVideoCaps(3840, 2160, 30))
            )
            .addMode("2.7K", sml.createMode()
                .addCapabilityList("sys_caps", getModeVideoCaps(2720, 1530, 30))
            )
            .addMode("FHD", sml.createMode()
                .addCapabilityList("sys_caps", getModeVideoCaps(1920, 1080, 96))
            )
            .addMode("HD", sml.createMode()
                .addCapabilityList("sys_caps", getModeVideoCaps(1280, 720, 120))
            )
            .addMode("Photo", sml.createMode()
                .addCapabilityList("sys_caps", getModeVideoCaps(4000, 3000, 0))
            )
            .build();
    }
    
    
    static CapabilityList getModeVideoCaps(int width, int height, int maxRate)
    {
        var builder = sml.capabilities.systemCapabilities()
            .label("Sensor Capabilities")
            .add("img_width", sml.createCount()
                .definition(RasterHelper.DEF_RASTER_WIDTH)
                .label("Image Width")
                .value(width)
            )
            .add("img_height", sml.createCount()
                .definition(RasterHelper.DEF_RASTER_HEIGHT)
                .label("Image Height")
                .value(height)
            );
        
        if (maxRate > 0)
        {
            builder.add("frame_rate", sml.createQuantityRange()
                .definition(CommonCapabilities.SAMPLING_FREQ_DEF)
                .label("Max Frame Rate")
                .uom("Hz")
                .value(24, maxRate)
            );
        }
        
        return builder.build();
    }
    
    
    static AbstractProcess createInsGpsDatasheet()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID(INSGPS_PROC_UID)
            .name("DJI Mavic Pro INS/GPS")
            .description("GPS/GLONASS aided inertial navigation system")
            
            .addIdentifier(sml.identifiers.longName("DJI Mavic Pro INS/GPS Unit"))
            .addClassifier(sml.classifiers.sensorType("INS"))
            .addClassifier(sml.classifiers.sensorType("GNSS"))
            
            .addInput("position", sml.createObservableProperty()
                .definition(GeoPosHelper.DEF_LOCATION)
                .label("Position")
                .build()
            )
            .addInput("attitude", sml.createObservableProperty()
                .definition(GeoPosHelper.DEF_ORIENTATION)
                .label("Attitude")
                .build()
            )
            .addInput("velocity", sml.createObservableProperty()
                .definition(GeoPosHelper.DEF_VELOCITY)
                .label("Velocity")
                .build()
            )

            .addCapabilityList("sys_caps", sml.capabilities.systemCapabilities()
                .label("Sensor Capabilities")
                .add("hacc", sml.capabilities.absoluteAccuracy(3, "m")
                    .label("Horizontal Position Accuracy"))
                .add("vacc", sml.capabilities.absoluteAccuracy(5, "m")
                    .label("Vertical Position Accuracy"))
                .add("res", sml.capabilities.resolution(0.1, "m"))
             )
            .addCapabilityList("op_range", sml.capabilities.operatingProperties()
                .add("temperature", sml.conditions.temperatureRange(0, 40, "Cel"))
             )
            .addContact(getDjiContactInfo())
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, getSpecWebPage())
            .build();
    }
    
    
    static AbstractProcess createPlatformInstance(String serialNum, Instant startTime)
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID("urn:x-osh:uav:mavic:" + serialNum)
            .name("Mavic Pro UAV " + serialNum)
            .typeOf(MAVICPRO_PROC_UID)
            .addIdentifier(sml.identifiers.shortName("Mavic Pro UAV"))
            .addIdentifier(sml.identifiers.serialNumber(serialNum))
            .addContact(getOperatorContactInfo())
            .validFrom(startTime.atOffset(ZoneOffset.UTC))
            .build();
    }
    
    
    static CIResponsibleParty getDjiContactInfo()
    {
        return sml.createContact()
            .role(CommonIdentifiers.MANUFACTURER_DEF)
            .organisationName("DJI Technology")
            .website("https://www.dji.com")
            .deliveryPoint("14th Floor, West Wing, Skyworth Semiconductor Design Building, No.18 Gaoxin South 4th Ave, Nanshan District")
            .city("Shenzhen")
            .postalCode("518057")
            .country("China")
            .email("dev@dji.com")
            .phone("+86 (0)755 26656677")
            .build();
    }
    
    
    static CIResponsibleParty getOperatorContactInfo()
    {
        return sml.createContact()
            .role(CommonIdentifiers.OPERATOR_DEF)
            .organisationName("OpenSensorHub")
            .website("https://www.opensensorhub.org")
            .country("USA")
            .email("info@opensensorhub.org")
            .build();
    }
    
    
    static CIOnlineResource getSpecWebPage()
    {
        return sml.createDocument()
            .name("Online Specs")
            .description("Webpage with drone specs and videos")
            .url("https://www.dji.com/mavic/info")
            .mediaType("text/html")
            .build();
    }

}
