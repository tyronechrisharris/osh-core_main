/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.procedure;

import java.io.IOException;
import org.isotc211.v2005.gmd.CIResponsibleParty;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import j2html.tags.DomContent;
import net.opengis.sensorml.v20.AbstractMetadataList;
import net.opengis.sensorml.v20.Term;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for system resources
 * </p>
 * 
 * @param <V> Type of SML feature resource
 * @param <DB> Database type
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public abstract class SmlFeatureBindingHtml<V extends IProcedureWithDesc, DB extends IDatabase> extends AbstractFeatureBindingHtml<V, DB>
{
    
    public SmlFeatureBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, DB db) throws IOException
    {
        super(ctx, idEncoders, isSummary, db);
    }
    
    
    protected void serializeDetails(FeatureKey key, IProcedureWithDesc sys) throws IOException
    {
        writeHeader();
        
        h3(sys.getName())
            .render(html);
        
        div(
            // UID
            h6(
                span("UID: ").withClass(CSS_BOLD),
                span(sys.getUniqueIdentifier())
            ),
        
            // system type def
            sys.getType() != null ?
                h6(
                    span("Type: ").withClass(CSS_BOLD),
                    a(sys.getType())
                        .withHref(sys.getType())
                        .withTarget(DICTIONARY_TAB_NAME)
                ) : null
        )
        .withClass("mt-4 mb-4")
        .render(html);
        
        var sml = sys.getFullDescription();
        
        // identification
        if (sml.getNumIdentifications() > 0)
        {
            renderCard(
                "Identification", 
                each(sml.getIdentificationList(), list ->
                    each(list.getIdentifierList(), term -> getTermHtml(term))
                ));
        }
        
        // classification
        if (sml.getNumClassifications() > 0)
        {
            renderCard(
                "Classification", 
                each(sml.getClassificationList(), list ->
                    each(list.getClassifierList(), term -> getTermHtml(term))
                ));
        }
        
        // characteristics
        if (sml.getNumCharacteristics() > 0)
        {
            for (var list: sml.getCharacteristicsList())
            {
                renderCard(
                    getListLabel(list, "Characteristics"), 
                    each(list.getCharacteristicList(), prop -> getComponentOneLineHtml(prop)));
            }
        }
        
        // capabilities
        if (sml.getNumCapabilities() > 0)
        {
            for (var list: sml.getCapabilitiesList())
            {
                renderCard(
                    getListLabel(list, "Capabilities"), 
                    each(list.getCapabilityList(), prop -> getComponentOneLineHtml(prop)));
            }
        }
        
        // contacts
        if (sml.getNumContacts() > 0)
        {
            for (var list: sml.getContactsList())
            {
                renderCard(
                    getListLabel(list, "Contacts"), 
                    each(list.getContactList(), contact -> getContactHtml(contact)));
            }
        }
        
        writeFooter();
        writer.flush();
    }
    
    
    DomContent getTermHtml(Term term)
    {
        return div(
            span(term.getLabel())
                .withTitle(term.getDefinition())
                .withClass("position-relative fw-bold"),
            span(": "),
            span(term.getValue()),
            sup(a(" ")
                .withClass("text-decoration-none bi-link")
                .withHref(term.getDefinition())
                .withTitle(term.getDefinition())
                .withTarget(DICTIONARY_TAB_NAME))
        ).withClass(CSS_CARD_TEXT);
    }
    
    
    DomContent getContactHtml(CIResponsibleParty contact)
    {
        String name = contact.getOrganisationName();
        if (name == null)
            name = contact.getIndividualName();
        
        var content = div();
        
        if (contact.isSetContactInfo())
        {
            var cInfo = contact.getContactInfo();
            
            if (cInfo.isSetAddress())
            {
                if (cInfo.getAddress().getNumDeliveryPoints() > 0)
                {
                    content.with(div(
                        span("Street").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getAddress().getDeliveryPointList().get(0))
                    )).withClass(CSS_CARD_TEXT);
                }
                
                if (cInfo.getAddress().isSetCity())
                {
                    content.with(div(
                        span("City").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getAddress().getCity())
                    )).withClass(CSS_CARD_TEXT);
                }
                
                if (cInfo.getAddress().isSetAdministrativeArea())
                {
                    content.with(div(
                        span("Administrative Area").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getAddress().getAdministrativeArea())
                    )).withClass(CSS_CARD_TEXT);
                }
                
                if (cInfo.getAddress().isSetPostalCode())
                {
                    content.with(div(
                        span("Postal Code").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getAddress().getPostalCode())
                    )).withClass(CSS_CARD_TEXT);
                }
                
                if (cInfo.getAddress().isSetCountry())
                {
                    content.with(div(
                        span("Country").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getAddress().getCountry())
                    )).withClass(CSS_CARD_TEXT);
                }
            }
            
            if (cInfo.isSetPhone())
            {
                for (int i = 0; i < cInfo.getPhone().getNumVoices(); i++)
                {
                    content.with(div(
                        span("Phone #").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getPhone().getVoiceList().get(i))
                    )).withClass(CSS_CARD_TEXT);
                }
                
                for (int i = 0; i < cInfo.getPhone().getNumFacsimiles(); i++)
                {
                    content.with(div(
                        span("Fax #").withClass(CSS_BOLD),
                        span(": "),
                        span(contact.getContactInfo().getPhone().getFacsimileList().get(i))
                    )).withClass(CSS_CARD_TEXT);
                }
            }
        }
        
        return getCard(name, content);
    }
    
    
    String getListLabel(AbstractMetadataList list, String defaultLabel)
    {
        if (list.getLabel() != null)
            return list.getLabel();
        
        return defaultLabel;
    }
}
