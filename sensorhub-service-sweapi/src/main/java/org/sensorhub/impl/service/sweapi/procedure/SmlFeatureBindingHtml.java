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
import org.sensorhub.api.feature.ISmlFeature;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import j2html.tags.DomContent;
import net.opengis.sensorml.v20.AbstractMetadataList;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.ObservableProperty;
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
public abstract class SmlFeatureBindingHtml<V extends ISmlFeature<?>, DB extends IDatabase> extends AbstractFeatureBindingHtml<V, DB>
{
    
    public SmlFeatureBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, DB db) throws IOException
    {
        super(ctx, idEncoders, isSummary, db);
    }
    
    
    protected void serializeDetails(FeatureKey key, V f) throws IOException
    {
        writeHeader();
        
        h3(f.getName())
            .render(html);
        
        div(
            // UID
            h6(
                span("UID: ").withClass(CSS_BOLD),
                span(f.getUniqueIdentifier())
            ),
        
            // system type def
            f.getType() != null ?
                h6(
                    span("Type: ").withClass(CSS_BOLD),
                    a(f.getType())
                        .withHref(f.getType())
                        .withTarget(DICTIONARY_TAB_NAME)
                ) : null
        )
        .withClass("mt-4 mb-4")
        .render(html);
        
        var sml = f.getFullDescription();
        
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
        
        // observables
        if (sml instanceof AbstractProcess)
        {
            var proc = (AbstractProcess)sml;
            if (proc.getNumInputs() > 0)
            {
                renderCard(
                    "Observed Properties", 
                    each(proc.getInputList(), input -> {
                        if (input instanceof ObservableProperty)
                        {
                            var def = ((ObservableProperty)input).getDefinition();
                            return div(
                                span(input.getLabel()).withClass(CSS_BOLD),
                                getLinkIcon(def, def)
                            );
                        }
                        else
                            return null;
                    }));
            }
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
            getLinkIcon(term.getDefinition(), term.getDefinition()),
            span(": "),
            span(term.getValue())
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
