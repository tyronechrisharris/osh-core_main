/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.system;

import java.io.IOException;
import org.isotc211.v2005.gmd.CIResponsibleParty;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.feature.AbstractFeatureBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import j2html.tags.DomContent;
import net.opengis.sensorml.v20.AbstractMetadataList;
import net.opengis.sensorml.v20.Term;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for system resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class SystemBindingHtml extends AbstractFeatureBindingHtml<ISystemWithDesc>
{
    final IObsSystemDatabase db;
    final boolean isSummary;
    final String collectionTitle;
    
    
    public SystemBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, String collectionTitle, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoder);
        this.db = db;
        this.isSummary = isSummary;
        
        if (ctx.getParentID() != 0L)
        {
            // fetch parent system name
            var parentSys = db.getSystemDescStore().getCurrentVersion(ctx.getParentID());
            this.collectionTitle = collectionTitle.replace("{}", parentSys.getName());
        }
        else
            this.collectionTitle = collectionTitle;
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    public void serialize(FeatureKey key, ISystemWithDesc sys, boolean showLinks) throws IOException
    {
        if (isSummary)
        {
            if (isCollection)
                serializeSummary(key, sys);
            else
                serializeSingleSummary(key, sys);
        }
        else
            serializeDetails(key, sys);
    }
    
    
    protected void serializeSingleSummary(FeatureKey key, ISystemWithDesc sys) throws IOException
    {
        writeHeader();
        serializeSummary(key, sys);
        writeFooter();
        writer.flush();
    }
    
    
    protected void serializeSummary(FeatureKey key, ISystemWithDesc sys) throws IOException
    {
        var sysId = Long.toString(encodeID(key.getInternalID()), ResourceBinding.ID_RADIX);
        var requestUrl = ctx.getRequestUrl();
        var resourceUrl = isCollection ? requestUrl + "/" + sysId : requestUrl;
        
        var hasSubSystems = db.getSystemDescStore().countMatchingEntries(new SystemFilter.Builder()
            .withParents(key.getInternalID())
            .withCurrentVersion()
            .build()) > 0;
            
        var hasFois = db.getFoiStore().countMatchingEntries(new FoiFilter.Builder()
            .withParents()
                .withInternalIDs(key.getInternalID())
                .includeMembers(true)
                .done()
            .includeMembers(true)
            .withCurrentVersion()
            .build()) > 0;
            
        var hasDataStreams = db.getDataStreamStore().countMatchingEntries(new DataStreamFilter.Builder()
            .withSystems()
                .withInternalIDs(key.getInternalID())
                .includeMembers(true)
                .done()
            .withCurrentVersion()
            .build()) > 0;
            
        var hasControls = db.getCommandStreamStore().countMatchingEntries(new CommandStreamFilter.Builder()
            .withSystems()
                .withInternalIDs(key.getInternalID())
                .includeMembers(true)
                .done()
            .withCurrentVersion()
            .build()) > 0;
        
        renderCard(
            a(sys.getName())
                .withHref(resourceUrl)
                .withClass("text-decoration-none"),
            iff(sys.getDescription() != null, div(
                sys.getDescription()
            ).withClasses(CSS_CARD_SUBTITLE)),
            div(
                span("UID: ").withClass(CSS_BOLD),
                span(sys.getUniqueIdentifier())
            ).withClass("mt-2"),
            iff(sys.getType() != null, div(
                span("System Type: ").withClass(CSS_BOLD),
                span(sys.getType().substring(sys.getType().lastIndexOf('/')+1))
            )),
            div(
                span("Validity Period: ").withClass(CSS_BOLD),
                getTimeExtentHtml(sys.getValidTime(), "Always")
            ).withClass("mt-2"),
            div(
                a("Spec Sheet").withHref(resourceUrl + "/details").withClasses(CSS_LINK_BTN_CLASSES),
                iff(hasSubSystems,
                    a("Subsystems").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasFois,
                    a("Sampling Features").withHref(resourceUrl + "/fois").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasDataStreams,
                    a("Datastreams").withHref(resourceUrl + "/datastreams").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasControls,
                    a("Control Channels").withHref(resourceUrl + "/controls").withClasses(CSS_LINK_BTN_CLASSES)),
                a("History").withHref(resourceUrl + "/history").withClasses(CSS_LINK_BTN_CLASSES)
            ).withClass("mt-4"));
    }
    
    
    protected void serializeDetails(FeatureKey key, ISystemWithDesc sys) throws IOException
    {
        writeHeader();
        
        h3(sys.getName()).render(html);
        
        // UID
        h6(
            span("UID: ").withClass(CSS_BOLD),
            span(sys.getUniqueIdentifier())
        ).render(html);
        
        // system type def
        if (sys.getType() != null)
        {
            h6(
                span("System Type: ").withClass(CSS_BOLD),
                span(sys.getType().substring(sys.getType().lastIndexOf('/')+1))
            ).render(html);
        }
        
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
                .withTarget("_blank"))
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
