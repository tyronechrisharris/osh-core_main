/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.sensorml;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import org.isotc211.v2005.gmd.CIOnlineResource;
import org.isotc211.v2005.gmd.CIResponsibleParty;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IDatabase;
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.feature.ISmlFeature;
import org.sensorhub.impl.service.consys.LinkResolver;
import org.sensorhub.impl.service.consys.feature.AbstractFeatureBindingHtml;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import com.google.common.base.Charsets;
import j2html.tags.DomContent;
import j2html.tags.specialized.DivTag;
import net.opengis.OgcProperty;
import net.opengis.gml.v32.Reference;
import net.opengis.sensorml.v20.AbstractMetadataList;
import net.opengis.sensorml.v20.AbstractPhysicalProcess;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.Mode;
import net.opengis.sensorml.v20.ModeChoice;
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
    
    public SmlFeatureBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, DB db, boolean showMap) throws IOException
    {
        super(ctx, idEncoders, isSummary, db, showMap);
    }
    
    
    protected DomContent getAlternateFormats()
    {
        var geoJsonQueryParams = new HashMap<>(ctx.getParameterMap());
        geoJsonQueryParams.remove("format"); // remove format in case it's set
        geoJsonQueryParams.put("f", new String[] {ResourceFormat.JSON.getMimeType()});
        
        var smlJsonQueryParams = new HashMap<>(ctx.getParameterMap());
        smlJsonQueryParams.remove("format"); // remove format in case it's set
        smlJsonQueryParams.put("f", new String[] {ResourceFormat.SML_JSON.getMimeType()});
        
        var smlXmlQueryParams = new HashMap<>(ctx.getParameterMap());
        smlXmlQueryParams.remove("format"); // remove format in case it's set
        smlXmlQueryParams.put("f", new String[] {ResourceFormat.SML_XML.getMimeType()});
        
        return span(
            a("GeoJSON").withHref(ctx.getRequestUrlWithQuery(geoJsonQueryParams)),
            text("/"),
            a("SensorML+JSON").withHref(ctx.getRequestUrlWithQuery(smlJsonQueryParams)),
            text("/"),
            a("SensorML+XML").withHref(ctx.getRequestUrlWithQuery(smlXmlQueryParams))
        );
    }
    
    
    protected void serializeDetails(FeatureKey key, V f) throws IOException
    {
        var sml = f.getFullDescription();
        var proc = (sml instanceof AbstractProcess)  ? (AbstractProcess)sml : null;
            
        writeHeader();
        
        h3(f.getName())
            .render(html);
        
        // links to sub resources
        var resourceUrl = getResourceUrl(key);
        var links = getLinks(resourceUrl, key);
        if (links != null && links.getNumChildren() > 0)
        {
            div(links)
                .withClass("mb-3 mt-3")
                .render(html);
        }
        
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
                ) : null,
                
            // procedure type
            proc != null && proc.isSetTypeOf() ?
                h6(
                    span(getTypeOfLabel(proc) + ": ").withClass(CSS_BOLD),
                    getTypeOfLink(proc.getTypeOf())
                ) : null,
                
           // description
           f.getDescription() != null ? div(
               i(f.getDescription())
           ).withClass("mt-3") : null
        )
        .withClass("mt-3")
        .render(html);
        
        html.appendStartTag("div")
            .appendAttribute("class", "accordion mt-3")
            .completeTag();
        
        // identification
        if (sml.getNumIdentifications() > 0)
        {
            getAccordionItem("Identification", div(
                each(sml.getIdentificationList(), list ->
                    each(list.getIdentifierList(), term -> getTermHtml(term))
                ))
            ).render(html);
        }
        
        // classification
        if (sml.getNumClassifications() > 0)
        {
            getAccordionItem("Classification", div(
                each(sml.getClassificationList(), list ->
                    each(list.getClassifierList(), term -> getTermHtml(term))
                ))
            ).render(html);
        }
        
        // observables
        if (proc != null)
        {
            if (proc.getNumInputs() > 0)
            {
                getAccordionItem(
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
                    })
                ).render(html);
            }
        }
        
        // capabilities
        if (sml.getNumCapabilities() > 0)
        {
            for (var list: sml.getCapabilitiesList())
            {
                getAccordionItem(
                    getListLabel(list, "Capabilities"),
                    each(list.getCapabilityList(), prop -> getComponentOneLineHtml(prop))
                ).render(html);
            }
        }
        
        // characteristics
        if (sml.getNumCharacteristics() > 0)
        {
            for (var list: sml.getCharacteristicsList())
            {
                getAccordionItem(
                    getListLabel(list, "Characteristics"),
                    each(list.getCharacteristicList(), prop -> getComponentOneLineHtml(prop))
                ).render(html);
            }
        }
        
        // contacts
        if (sml.getNumContacts() > 0)
        {
            getAccordionItem("Contacts", true, div(
                each(sml.getContactsList(), list ->
                    each(list.getContactList(), (i, contact) -> getContactHtml(i, contact))
                ))
            ).render(html);
        }
        
        // documents
        if (sml.getNumDocumentations() > 0)
        {
            getAccordionItem("Documents", false, div(
                each(sml.getDocumentationList(), list ->
                    each(list.getDocumentList().getProperties(), (i, prop) -> getDocumentHtml(i, prop))
                ))
            ).render(html);
        }
        
        // modes
        if (proc != null)
        {
            if (proc.getNumModes() > 0)
            {
                getAccordionItem("Modes", false, div(
                    each(proc.getModesList(), list ->
                        each(((ModeChoice)list).getModeList(), mode -> getModeHtml(mode))
                    ))
                ).render(html);
            }
        }
        
        html.appendEndTag("div");
        
        /*
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
        if (proc != null)
        {
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
                    "Contacts", 
                    each(list.getContactList(), contact -> getContactHtml(contact)));
            }
        }
        
        // documents
        if (sml.getNumDocumentations() > 0)
        {
            for (var list: sml.getDocumentationList())
            {
                renderCard(
                    "Documents", 
                    each(list.getDocumentList().getProperties(), doc -> getDocumentHtml(doc)));
            }
        }
        
        // modes
        if (proc != null && !proc.getModesList().isEmpty())
        {
            var content = div(
                h5(
                    a("Modes")
                      .withHref("#MODE_LIST")
                      .attr("data-bs-toggle", "collapse")
                )
            ).withClass("mt-3");
            
            var modeList = div()
                .withClass("collapse")
                .withId("MODE_LIST");
            
            for (var list: proc.getModesList())
            {
                modeList.with(
                    each(((ModeChoice)list).getModeList(), mode -> getModeHtml(mode))
                );
            }
            
            content.with(modeList);
            content.render(html);
        }*/
        
        writeFooter();
        writer.flush();
    }
    
    
    DivTag getAccordionItem(String title, DomContent content)
    {
        return getAccordionItem(title, true, content);
    }
    
    
    DivTag getAccordionItem(String title, boolean show, DomContent content)
    {
        String itemId = "acc" + Integer.toHexString((int)(Math.random()*100000000));
        
        return div()
            .withClass("accordion-item")
            .with(
                h2()
                    .withId("h" + itemId)
                    .withClass("accordion-header")
                    .with(
                        button(title)
                            .withClass("accordion-button" + (show ? "" : " collapsed"))
                            .attr("type", "button")
                            .attr("data-bs-toggle", "collapse")
                            .attr("data-bs-target", "#" + itemId)
                    ),
                div()
                    .withId(itemId)
                    .withClass("accordion-collapse collapse" + (show ? " show" : ""))
                    .with(
                        div(content)
                            .withClass("accordion-body")
                    )
            );
    }
    
    
    String getTypeOfLabel(AbstractProcess proc)
    {
        if (proc instanceof AbstractPhysicalProcess)
            return "Model";
        else
            return "Method";
    }
    
    
    DomContent getTypeOfLink(Reference ref)
    {
        if (db instanceof IProcedureDatabase) {
            LinkResolver.resolveTypeOf(ctx, ref, (IProcedureDatabase)db, idEncoders);
        }
        
        String title = ref.getTitle();
        if (title == null && ref.getRole() != null)
            title = ref.getRole();
        if (title == null)
            title = ref.getHref();
        return a(title).withHref(ref.getHref());
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
    
    
    DomContent getModeHtml(Mode mode)
    {
        var content = div();
        
        if (mode.isSetDescription())
        {
            
        }
        
        // characteristics
        if (mode.getNumCharacteristics() > 0)
        {
            for (var list: mode.getCharacteristicsList())
            {
                content.with(getCard(
                    getListLabel(list, "Characteristics"), 
                    each(list.getCharacteristicList(), prop -> getComponentOneLineHtml(prop)))
                );
            }
        }
        
        // capabilities
        if (mode.getNumCapabilities() > 0)
        {
            for (var list: mode.getCapabilitiesList())
            {
                content.with(getCard(
                    getListLabel(list, "Capabilities"), 
                    each(list.getCapabilityList(), prop -> getComponentOneLineHtml(prop)))
                );
            }
        }
        
        
        return getCard("Mode " + mode.getName(), content);
    }
    
    
    DomContent getContactHtml(int idx, CIResponsibleParty contact)
    {
        String name = contact.getOrganisationName();
        if (name == null)
            name = contact.getIndividualName();
        
        var content = div();
        
        if (contact.getRole() != null)
        {
            var roleUri = contact.getRole().getValue();
            content.with(div(
                span("Role").withClass(CSS_BOLD),
                span(": "),
                span(getLabelFromUri(roleUri)),
                getLinkIcon(roleUri, roleUri)
            )).withClass(CSS_CARD_TEXT);
        }
        
        if (contact.isSetContactInfo())
        {
            var cInfo = contact.getContactInfo();
            
            if (cInfo.isSetOnlineResource())
            {
                var link = contact.getContactInfo().getOnlineResource().getLinkage();
                content.with(div(
                    span("Website").withClass(CSS_BOLD),
                    span(": "),
                    span(a(link).withHref(link))
                )).withClass(CSS_CARD_TEXT);
            }
            
            if (cInfo.isSetAddress())
            {
                if (cInfo.getAddress().getNumDeliveryPoints() > 0)
                {
                    content.with(div(
                        span("Street Address").withClass(CSS_BOLD),
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
                
                for (var email: cInfo.getAddress().getElectronicMailAddressList())
                {
                    content.with(div(
                        span("Email").withClass(CSS_BOLD),
                        span(": "),
                        span(a(email)
                            .withHref("mailto:"+email)
                            .withTarget(BLANK_TAB))
                    )).withClass(CSS_CARD_TEXT);
                }
            }
            
            if (cInfo.isSetPhone())
            {
                for (var num: cInfo.getPhone().getVoiceList())
                {
                    content.with(div(
                        span("Phone").withClass(CSS_BOLD),
                        span(": "),
                        span(num)
                    )).withClass(CSS_CARD_TEXT);
                }
                
                for (var num: cInfo.getPhone().getFacsimileList())
                {
                    content.with(div(
                        span("Fax").withClass(CSS_BOLD),
                        span(": "),
                        span(num)
                    )).withClass(CSS_CARD_TEXT);
                }
            }
        }
        
        //return getCard(h5(small(name)).withClass(CSS_BOLD), content);
        return getSection(idx, name, content);
    }
    
    
    DomContent getDocumentHtml(int idx, OgcProperty<CIOnlineResource> prop)
    {
        var content = div();
        
        var doc = prop.getValue();
        var name = doc.getName() != null ? doc.getName() : "Document";
        
        if (prop.getRole() != null)
        {
            var roleUri = prop.getRole();
            content.with(div(
                span("Type").withClass(CSS_BOLD),
                span(": "),
                span(getLabelFromUri(roleUri)),
                getLinkIcon(roleUri, roleUri)
            )).withClass(CSS_CARD_TEXT);
        }
        
        if (doc.isSetDescription())
        {
            content.with(div(
                span("Description").withClass(CSS_BOLD),
                span(": "),
                span(doc.getDescription())
            )).withClass(CSS_CARD_TEXT);
        }
        
        if (doc.getLinkage() != null)
        {
            var link = doc.getLinkage();
            content.with(div(
                span("Link").withClass(CSS_BOLD),
                span(": "),
                span(a(getPrettyLink(link)).withHref(link))
            )).withClass(CSS_CARD_TEXT);
        }
        
        //return getCard(h5(small(name)).withClass(CSS_BOLD), content);
        return getSection(idx, name, content);
    }
    
    
    DivTag getSection(int idx, String title, DomContent content)
    {
        return div(
            h5(small(title)).withClass(CSS_BOLD),
            div(content).withClass("ms-3")
        ).withClass(idx > 0 ? "mt-4 mb-1" : "mt-1 mb-1");
    }
    
    
    String getLabelFromUri(String uri)
    {
        var name = uri.substring(uri.lastIndexOf('/')+1);
        return name.replaceAll("_", " ");
    }
    
    
    String getPrettyLink(String url)
    {
        if (url != null) 
        {
            url = URLDecoder.decode(url, Charsets.UTF_8);
            if (url.length() > 60)
                url = url.substring(0, 60) + "...";
        }
        
        return url;
    }
    
    
    String getListLabel(AbstractMetadataList list, String defaultLabel)
    {
        if (list.getLabel() != null)
            return list.getLabel();
        
        return defaultLabel;
    }
}
