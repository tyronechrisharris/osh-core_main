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

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLMetadataBuilders.CIResponsiblePartyBuilder;
import org.vast.sensorML.helper.CommonIdentifiers;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import org.vast.swe.helper.GeoPosHelper;
import net.opengis.sensorml.v20.AbstractProcess;


public class Pleiades
{
    public static final String PHR_PROC_UID = "urn:x-cnes:sat:phr";
    public static final String PHR1A_SYS_UID = "urn:x-cnes:sat:phr:1a";
    public static final String PHR1B_SYS_UID = "urn:x-cnes:sat:phr:1b";
    public static final String HIRI_PROC_UID = "urn:x-cnes:ins:hiri";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    static AbstractProcess createPHRSpecs()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID(PHR_PROC_UID)
            .name("Pleiades-HR")
            .description("Pléiades is the successor of the SPOT program, with a focus on cost reduction, "
                + "technological innovation, user services and performance upgrades. The constellation provides "
                + "global coverage and daily observation accessibility to any point on Earth in several modes "
                + "of operation. Pléiades satellites are based on the agile Astrosat-1000 platform and fitted with "
                + "CNES high-resolution imager (HiRI) developed by Thales Alenia Space (TAS-F). The instrument's "
                + "prime objective is to provide  high-resolution multispectral imagery with high geo-location "
                + "accuracy. It offers several types of acquisition modes to fulfil various application fields "
                + "of cartography, agriculture, forestry, hydrology, geological prospecting, dynamic geology, "
                + "risk management and defence.")
            
            .addIdentifier(sml.identifiers.shortName("Pleiades-HR"))
            .addIdentifier(sml.identifiers.longName("Pleiades-HR Satellite Platform"))
            .addIdentifier(sml.identifiers.author("CNES"))
            
            .addCharacteristicList("physical", sml.createCharacteristicList()
                .label("Physical Characteristics")
                .add("weight", sml.characteristics.mass(970, "kg"))
            )
            
            .addCharacteristicList("orbit", sml.createCharacteristicList()
                .label("Orbit Characteristics")
                .add("type", sml.createCategory()
                    .definition(SWEHelper.getPropertyUri("OrbitType"))
                    .label("Orbit Type")
                    .value("Sun-synchronous"))
                .add("height", sml.characteristics.height(694, "km"))
                .add("inclination", sml.createQuantity()
                    .definition(SWEHelper.getPropertyUri("OrbitInclination"))
                    .label("Inclination")
                    .uomCode("deg")
                    .value(98.2))
                .add("repeat_cycle", sml.createQuantity()
                    .definition(SWEHelper.getPropertyUri("OrbitRepeatCycle"))
                    .label("Repeat Cycle")
                    .uomCode("d")
                    .value(26))
            )
            
            .addCapabilityList("pointing_specs", sml.capabilities.systemCapabilities()
                .label("Pointing Capabilities")
                .add("roll_range", sml.capabilities.pointingRange(-30, 30, "deg")
                    .label("Across-Track Pointing Range (Roll)"))
                .add("pitch_range", sml.capabilities.pointingRange(-30, 30, "deg")
                    .label("Along-Track Pointing Range (Pitch)"))
                .add("maneuver_speed", sml.createQuantity()
                    .definition(GeoPosHelper.DEF_ANGULAR_VELOCITY)
                    .label("Maneuver Speed")
                    .uom("deg/s")
                    .value(2.0))
            )
            
            .addCapabilityList("imaging_specs", sml.capabilities.systemCapabilities()
                .label("Imaging Capabilities")
                .add("swath_width", sml.createQuantity()
                    .definition(SWEHelper.getPropertyUri("SwathWidth"))
                    .label("Swath Width")
                    .description("Swath width at nadir")
                    .uom("km")
                    .value(20.0))
                .add("res_pan", sml.createQuantity()
                    .definition(SWEHelper.getPropertyUri("GroundSamplingDistance"))
                    .label("Spatial Resolution, GSD (PAN)")
                    .uom("m")
                    .value(0.7))
                .add("res_ms", sml.createQuantity()
                    .definition(SWEHelper.getPropertyUri("GroundSamplingDistance"))
                    .label("Spatial Resolution, GSD (MS)")
                    .uom("m")
                    .value(2.8))
            )
            
            .addContact(Spot.getCnesContactInfo()
                .role(CommonIdentifiers.AUTHOR_DEF)
            )
            .addContact(Spot.getAirbusContactInfo()
                .role(CommonIdentifiers.MANUFACTURER_DEF)
            )
            
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, sml.createDocument()
                .name("Pléiades EO-Portal Page")
                .description("Pleiades page with specs on eo-portal website")
                .url("https://www.eoportal.org/satellite-missions/pleiades")
                .mediaType("text/html")
            )
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, sml.createDocument()
                .name("Pléiades Wikipedia Page")
                .url("https://en.wikipedia.org/wiki/Pleiades_(satellite)")
                .mediaType("text/html")
            )
            
            .addComponent("HiRI", createHIRISpecs())
            
            .build();
    }
    
    
    static AbstractProcess createHIRISpecs()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_SENSOR)
            .uniqueID(HIRI_PROC_UID)
            .name("Pleiades High Resolution Imager")
            .description("HiRI captures images in one panchromatic (PAN) and four multispectral bands: blue, green, "
                + "red and near-infrared. The PAN band has a spatial resolution of 0.7 m whilst the multispectral "
                + "bands have a spatial resolution of 2.8 m. The swath width for each is 20 km at nadir.")
            
            .addIdentifier(sml.identifiers.shortName("HiRI"))
            .addIdentifier(sml.identifiers.longName("Pleiades High Resolution Imager"))
            .addClassifier(sml.classifiers.sensorType("Optical Pushbroom Imager"))
            
            .addInput("radiance", sml.createObservableProperty()
                .definition(SWEHelper.getQudtUri("Radiance"))
                .label("Earth Radiance")
                .build()
            )
            
            .addCharacteristicList("physical", sml.createCharacteristicList()
                .label("Physical Characteristics")
                .add("weight", sml.characteristics.mass(195, "kg"))
                .add("length", sml.characteristics.length(1594, "mm"))
                .add("width", sml.characteristics.width(980, "mm"))
                .add("height", sml.characteristics.height(2235, "mm"))
            )
            
            .addCapabilityList("optical_specs", sml.capabilities.systemCapabilities()
                .label("Optical Capabilities")
                .add("resolution", sml.capabilities.resolution(12, "bit"))
                .add("focal", sml.capabilities.focalLength(12.905, "m"))
                .add("fov", sml.capabilities.fov(1.65, "deg"))
            )
            
            .addCapabilityList("pan_specs", sml.capabilities.systemCapabilities()
                .label("Panchromatic Band Capabilities")
                .add("spectal_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Wavelength"))
                    .label("Spectral Range")
                    .uom("nm")
                    .value(480, 820))
                .add("snr", sml.capabilities.snr(147))
            )
            
            .addCapabilityList("band0_specs", sml.capabilities.systemCapabilities()
                .label("MS Band 0 Capabilities (blue)")
                .add("spectal_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Wavelength"))
                    .label("Spectral Range")
                    .uom("nm")
                    .value(450, 530))
                .add("snr", sml.capabilities.snr(130))
            )
            
            .addCapabilityList("band1_specs", sml.capabilities.systemCapabilities()
                .label("MS Band 1 Capabilities (green)")
                .add("spectal_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Wavelength"))
                    .label("Spectral Range")
                    .uom("nm")
                    .value(510, 590))
                .add("snr", sml.capabilities.snr(130))
            )
            
            .addCapabilityList("band2_specs", sml.capabilities.systemCapabilities()
                .label("MS Band 2 Capabilities (red)")
                .add("spectal_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Wavelength"))
                    .label("Spectral Range")
                    .uom("nm")
                    .value(620, 700))
                .add("snr", sml.capabilities.snr(130))
            )
            
            .addCapabilityList("band3_specs", sml.capabilities.systemCapabilities()
                .label("MS Band 3 Capabilities (NIR)")
                .add("spectal_range", sml.createQuantityRange()
                    .definition(SWEHelper.getQudtUri("Wavelength"))
                    .label("Spectral Range")
                    .uom("nm")
                    .value(775, 915))
                .add("snr", sml.capabilities.snr(130))
            )
            
            .addContact(getThalesContactInfo()
                .role(CommonIdentifiers.MANUFACTURER_DEF)
            )
            
            .build();
    }
    
    
    static Collection<AbstractProcess> getPHRInstances()
    {
        var list = new ArrayList<AbstractProcess>(100);
        
        list.add(sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID(PHR1A_SYS_UID)
            .name("Pléiades-1A Satellite")
            .description("First satellite of the Pléiades-HR constellation developped by CNES.")
            .typeOf(PHR_PROC_UID)
            .addIdentifier(sml.identifiers.shortName("Pléiades-1A"))
            .addIdentifier(sml.identifiers.operator("CNES"))
            .addIdentifier(sml.createTerm()
                .definition(SWEHelper.getDBpediaUri("International_Designator"))
                .label("International Designator (COSPAR ID)")
                .value("2011-076F")
            )
            .addIdentifier(sml.createTerm()
                .definition(SWEHelper.getDBpediaUri("Satellite_Catalog_Number"))
                .label("Satellite Catalog Number (SATCAT, NORAD)")
                .value("38012")
            )
            .addContact(Spot.getCnesContactInfo()
                .role(CommonIdentifiers.OPERATOR_DEF))
            .validFrom(OffsetDateTime.parse("2011-12-17T02:03:00Z"))
            .build());
        
        list.add(sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID(PHR1B_SYS_UID)
            .name("Pléiades-1B Satellite")
            .description("Second satellite of the Pléiades-HR constellation developped by CNES.")
            .typeOf(PHR_PROC_UID)
            .addIdentifier(sml.identifiers.shortName("Pléiades-1B"))
            .addIdentifier(sml.identifiers.operator("CNES"))
            .addIdentifier(sml.createTerm()
                .definition(SWEHelper.getDBpediaUri("International_Designator"))
                .label("International Designator (COSPAR ID)")
                .value("2012-068A")
            )
            .addIdentifier(sml.createTerm()
                .definition(SWEHelper.getDBpediaUri("Satellite_Catalog_Number"))
                .label("Satellite Catalog Number (SATCAT/NORAD)")
                .value("39019")
            )
            .addContact(Spot.getCnesContactInfo()
                .role(CommonIdentifiers.OPERATOR_DEF))
            .validFrom(OffsetDateTime.parse("2012-12-02T02:02:00Z"))
            .build());
        
        return list;
    }
    
    
    static CIResponsiblePartyBuilder getThalesContactInfo()
    {
        return sml.createContact()
            .organisationName("Thales Alenia Space")
            .website("https://www.thalesgroup.com")
            .deliveryPoint("26 Av. Jean François Champollion")
            .city("Toulouse")
            .postalCode("31100")
            .country("France")
            .phone("+ 33 (0)5 34 35 36 37");
    }

}
