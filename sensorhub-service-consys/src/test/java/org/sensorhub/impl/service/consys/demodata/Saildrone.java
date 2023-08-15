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
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLMetadataBuilders.CIResponsiblePartyBuilder;
import org.vast.sensorML.helper.CommonIdentifiers;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import org.vast.swe.helper.GeoPosHelper;
import net.opengis.sensorml.v20.AbstractProcess;


public class Saildrone
{
    public static final String PLATFORM_PROC_UID = "urn:x-saildrone:platform:explorer";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    static AbstractProcess createPlatformDatasheet()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID(PLATFORM_PROC_UID)
            .name("Saildrone Explorer")
            .description("The Saildrone Explorer is a 23-foot (7 m) vehicle powered\n"
                + "by wind and solar energy capable of extreme-duration\n"
                + "missions over 12 months in the open ocean, while\n"
                + "producing a minimal carbon footprint. Sailing at an\n"
                + "average speed up to three knots, the Explorer carries a\n"
                + "suite of scientific sensors for the collection of ocean data.")
            
            .addIdentifier(sml.identifiers.shortName("Saildrone Explorer"))
            .addIdentifier(sml.identifiers.modelNumber("Explorer"))
            
            .addCharacteristicList("physical", sml.createCharacteristicList()
                .label("Physical Characteristics")
                .add("weight", sml.characteristics.mass(750, "kg"))
                .add("length", sml.characteristics.length(7, "m")
                    .label("Hull Length"))
                .add("width", sml.characteristics.width(0.9, "m")
                    .label("Hull Width"))
                .add("height", sml.characteristics.height(5, "m")
                    .label("Wing Height"))
                .add("draft", sml.createQuantity()
                    .definition(SWEHelper.getDBpediaUri("Draft_(hull)"))
                    .label("Draft")
                    .uom("m")
                    .value(2))
             )
            .addCapabilityList("nav_capabilities", sml.capabilities.systemCapabilities()
                .add("avg_speed", sml.createQuantityRange()
                    .definition(SWEHelper.getPropertyUri("SpeedOverWater"))
                    .label("Average Speed")
                    .uomCode("[kn_i]")
                    .value(2, 3)
                )
                .add("max_speed", sml.createQuantity()
                    .definition(SWEHelper.getPropertyUri("SpeedOverWater"))
                    .label("Top Speed")
                    .uomCode("[kn_i]")
                    .value(8)
                )
                .add("endurance", sml.createQuantity()
                    .definition(SWEHelper.getDBpediaUri("Endurance"))
                    .description("Maximum mission duration")
                    .uom("d")
                    .value(365)
                )
             )
            .addCapabilityList("op_range", sml.capabilities.operatingProperties()
                .add("temperature", sml.conditions.temperatureRange(-50, 100, "Cel"))
                .add("humidity", sml.conditions.humidityRange(0, 100, "%"))
                .add("wind_speed", sml.createQuantity()
                    .definition(SWEHelper.getCfUri("wind_speed"))
                    .label("Max Wind Speed")
                    .uomCode("km/h")
                    .value(250)
                )
             )
            
            .addContact(getSaildroneContactInfo()
                .role(CommonIdentifiers.MANUFACTURER_DEF)
            )
            
            .addDocument(CommonIdentifiers.WEBPAGE_DEF, sml.createDocument()
                .name("Product Web Site")
                .description("Webpage with general info about Saildrone vehicles")
                .url("https://www.saildrone.com/technology/vehicles")
                .mediaType("text/html")
            )
            .addDocument(CommonIdentifiers.SPECSHEET_DEF, sml.createDocument()
                .name("Spec Sheet")
                .url("https://indd.adobe.com/view/cb870469-0058-408d-828a-9d83e49c8d79")
                .mediaType("application/pdf")
             )
            .build();
    }
    
    
    static AbstractProcess createPlatformInstance(String serialNum, Instant startTime)
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PLATFORM)
            .uniqueID("urn:x-osh:usv:saildrone:" + serialNum)
            .name("Saildrone USV SD-" + serialNum)
            .typeOf(PLATFORM_PROC_UID)
            .addIdentifier(sml.identifiers.shortName("Saildrone Explorer USV"))
            .addIdentifier(sml.identifiers.serialNumber("SD-"+ serialNum))
            .addContact(getSaildroneContactInfo().role(CommonIdentifiers.OPERATOR_DEF))
            .validFrom(startTime.atOffset(ZoneOffset.UTC))
            .build();
    }
    
    
    static CIResponsiblePartyBuilder getSaildroneContactInfo()
    {
        return sml.createContact()
            .organisationName("Saildrone, Inc.")
            .website("https://www.saildrone.com")
            .deliveryPoint("1050 W. Tower Ave.")
            .city("Alameda")
            .postalCode("94501")
            .administrativeArea("CA")
            .country("USA")
            .email("info@saildrone.com");
    }
    
    /***********/
    
    

}
