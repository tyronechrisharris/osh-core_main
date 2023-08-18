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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLMetadataBuilders.CIResponsiblePartyBuilder;
import org.vast.sensorML.helper.CommonIdentifiers;
import org.vast.swe.SWEConstants;
import org.vast.swe.helper.GeoPosHelper;
import net.opengis.sensorml.v20.AbstractProcess;


public class Humans
{
    public static final String BIRD_SURVEY_PROC_UID = "urn:x-nsw:environment:survey:birds";
    public static final String WATER_SAMPLING_PROC_UID = "urn:x-usgs:environment:survey:water";
    
    static SMLHelper sml = new SMLHelper();
    static GeoPosHelper swe = new GeoPosHelper();
    
    
    static AbstractProcess createBirdSurveyProcedure()
    {
        return sml.createSimpleProcess()
            .definition(SWEConstants.DEF_PROCEDURE)
            .uniqueID(BIRD_SURVEY_PROC_UID)
            .name("Bird Survey - Baseline Method")
            .description("The Baseline method is based on setting up a single line at each "
                + "site called a transect. Birds can be identified either visually, or by their calls. This "
                + "method involves identifying all the birds you see or hear while standing at a series of "
                + "points along a transect (a straight line through the site).")
            .addContact(getNswNpaContactInfo()
                .role(CommonIdentifiers.AUTHOR_DEF)
            )
            .addDocument(CommonIdentifiers.REPORT_DEF, sml.createDocument()
                .name("Survey Method Document")
                .description("Document describing the Baseline method for bird surveys")
                .url("https://www.environment.nsw.gov.au/resources/howyoucanhelp/09birdsurveysbaseline.pdf")
                .mediaType("application/pdf")
            )
            .build();
    }
    
    
    static AbstractProcess createWaterSamplingProcedure()
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_PROCEDURE)
            .uniqueID(WATER_SAMPLING_PROC_UID)
            .name("USGS Water Sampling Procedure")
            .description("Methodology for collecting water samples that are later analyzed "
                + "in a laboratory.")
            .addContact(getUsgsContactInfo(null)
                .role(CommonIdentifiers.AUTHOR_DEF)
            )
            .addDocument(CommonIdentifiers.REPORT_DEF, sml.createDocument()
                .name("Methodology Document")
                .description("Document describing the sampling methodology")
                .url("https://www.water.usgs.gov/...")
                .mediaType("application/pdf")
            )
            .build();
    }
    
    
    static AbstractProcess createHumanBirdWatcher(String id, String name, Instant startTime)
    {
        return sml.createPhysicalSystem()
            .definition(SWEConstants.DEF_OBSERVER)
            .uniqueID("urn:x-npansw:surveyor:" + id)
            .name("Bird Watcher NPA" + id + " (" + name + ")")
            .typeOf(BIRD_SURVEY_PROC_UID)
            .addIdentifier(sml.identifiers.serialNumber("NPA" + id))
            .addIdentifier(sml.identifiers.longName(name))
            .validFrom(startTime.atOffset(ZoneOffset.UTC))
            .build();
    }
    
    
    static Collection<AbstractProcess> getAllBirdWatchers()
    {
        var list = new ArrayList<AbstractProcess>();
        
        var names = new String[] {
            "Alice Garrison",
            "Bria Kerr",
            "Winston James",
            "Davon Schmitt",
            "Arturo Webster"
        };
        
        for (int i = 0; i < names.length; i++)
        {
            list.add(createHumanBirdWatcher(
                "123" + i,
                names[i],
                Instant.parse("2010-09-12T08:00:00Z").plus(i*i, ChronoUnit.DAYS)));
        }
        
        return list;
    }
    
    
    static CIResponsiblePartyBuilder getNswNpaContactInfo()
    {
        return sml.createContact()
            .organisationName("National Park Association of New South Wales")
            .website("https://npansw.org.au")
            .deliveryPoint("PO Box 528")
            .city("Pyrmont")
            .administrativeArea("NSW")
            .postalCode("2009")
            .country("Australia")
            .email("npansw@npansw.org.au");
    }
    
    
    static CIResponsiblePartyBuilder getUsgsContactInfo(String personName)
    {
        return sml.createContact()
            .individualName(personName)
            .organisationName("U.S. Geological Survey")
            .website("https://water.usgs.gov/nawqa")
            .deliveryPoint("413 National Center")
            .city("Reston")
            .administrativeArea("VA")
            .postalCode("20192")
            .country("USA")
            .email("gs-w_nawqa_whq@usgs.gov");
    }

}
