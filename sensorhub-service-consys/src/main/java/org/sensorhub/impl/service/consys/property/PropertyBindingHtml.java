/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.property;

import java.io.IOException;
import java.util.HashMap;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.datastore.property.PropertyKey;
import org.sensorhub.api.semantic.IDerivedProperty;
import org.sensorhub.impl.service.consys.procedure.ProcedureHandler;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.resource.ResourceBindingHtml;
import org.sensorhub.impl.service.consys.resource.ResourceFormat;
import org.sensorhub.impl.service.consys.system.SystemHandler;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for property resources
 * </p>
 *
 * @author Alex Robin
 * @since Oct 8, 2023
 */
public class PropertyBindingHtml extends ResourceBindingHtml<PropertyKey, IDerivedProperty>
{
    final boolean isSummary;
    final String collectionTitle;
    
    
    public PropertyBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, String collectionTitle) throws IOException
    {
        super(ctx, idEncoders);
        this.isSummary = isSummary;
        this.collectionTitle = collectionTitle;
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    public void serialize(PropertyKey key, IDerivedProperty dsInfo, boolean showLinks) throws IOException
    {
        if (isSummary)
        {
            if (isCollection)
                serializeSummary(key, dsInfo);
            else
                serializeSingleSummary(key, dsInfo);
        }
        else
            serializeDetails(key, dsInfo);
    }
    
    
    protected void serializeSummary(PropertyKey key, IDerivedProperty prop) throws IOException
    {
        var propId = idEncoders.getPropertyIdEncoder().encodeID(key.getInternalID());
        var propUrl = ctx.getApiRootURL() + "/" + PropertyHandler.NAMES[0] + "/" + propId;
        
        var propSearchUrl = ctx.getApiRootURL() + "/" + PropertyHandler.NAMES[0] + "?baseProperty=" + prop.getURI();
        var procSearchUrl = ctx.getApiRootURL() + "/" + ProcedureHandler.NAMES[0] + "?observedProperty=" + prop.getURI();
        var sysSearchUrl = ctx.getApiRootURL() + "/" + SystemHandler.NAMES[0] + "?observedProperty=" + prop.getURI();
        
        renderCard(
            a(prop.getName())
                .withHref(propUrl)
                .withClass("text-decoration-none"),
            iff(prop.getDescription() != null, div(
                prop.getDescription()
            ).withClasses(CSS_CARD_SUBTITLE)),
            div(
                span("Base Property: ").withClass(CSS_BOLD),
                a(getPrettyNameFromUri(prop.getBaseProperty())).withHref(prop.getBaseProperty())
            ),
            prop.getObjectType() != null ? div(
                span("Object Type: ").withClass(CSS_BOLD),
                a(getPrettyNameFromUri(prop.getObjectType())).withHref(prop.getObjectType())
            ) : null,
            prop.getStatistic() != null ? div(
                span("Applied Statistic: ").withClass(CSS_BOLD),
                a(getPrettyNameFromUri(prop.getStatistic())).withHref(prop.getStatistic())
            ) : null,
            !prop.getQualifiers().isEmpty() ? div(
                span("Qualifiers: ").withClass(CSS_BOLD),
                div(
                    each(prop.getQualifiers(), q -> getComponentOneLineHtml(q))
                ).withClass("ps-4")
            ): null,
            p(
                a("Derived Properties")
                    .withTitle("Properties derived from this property")
                    .withHref(propSearchUrl)
                    .withClasses(CSS_LINK_BTN_CLASSES),
                a("Observing Procedures")
                    .withTitle("Types of systems or methods that can be used to observe this property")
                    .withHref(procSearchUrl)
                    .withClasses(CSS_LINK_BTN_CLASSES),
                a("Observing Systems")
                    .withTitle("Systems that can observe this property")
                    .withHref(sysSearchUrl)
                    .withClasses(CSS_LINK_BTN_CLASSES)
            ).withClass("mt-4")
         );
    }
    
    
    String getPrettyNameFromUri(String uri)
    {
        var idx = uri.lastIndexOf('/');
        if (idx < 0)
            idx = uri.lastIndexOf(':');
        return idx > 0 ? uri.substring(idx+1) : uri;
    }
    
    
    protected void serializeSingleSummary(PropertyKey key, IDerivedProperty dsInfo) throws IOException
    {
        writeHeader();
        serializeSummary(key, dsInfo);
        writeFooter();
        writer.flush();
    }
    
    
    protected void serializeDetails(PropertyKey key, IDerivedProperty dsInfo) throws IOException
    {
        serializeSummary(key, dsInfo);
    }
    
    
    @Override
    protected DomContent getAlternateFormats()
    {
        var geoJsonQueryParams = new HashMap<>(ctx.getParameterMap());
        geoJsonQueryParams.remove("format"); // remove format in case it's set
        geoJsonQueryParams.put("f", new String[] {ResourceFormat.JSON.getMimeType()});
        
        var smlJsonQueryParams = new HashMap<>(ctx.getParameterMap());
        smlJsonQueryParams.remove("format"); // remove format in case it's set
        smlJsonQueryParams.put("f", new String[] {"text/turtle"});
        
        return span(
            a("JSON").withHref(ctx.getRequestUrlWithQuery(geoJsonQueryParams)),
            text("/"),
            a("RDF-Turtle").withHref(ctx.getRequestUrlWithQuery(smlJsonQueryParams))
        );
    }
}
