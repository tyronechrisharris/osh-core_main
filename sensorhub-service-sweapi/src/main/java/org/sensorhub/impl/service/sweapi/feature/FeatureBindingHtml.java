/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.feature;

import java.io.IOException;
import java.util.Map.Entry;
import javax.xml.namespace.QName;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.sensorhub.impl.service.sweapi.system.SystemHistoryHandler;
import org.vast.ogc.gml.GMLUtils;
import org.vast.ogc.gml.GenericTemporalFeatureImpl;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.xlink.IXlinkReference;
import j2html.tags.DomContent;
import j2html.tags.Tag;
import j2html.tags.UnescapedText;
import net.opengis.gml.v32.AbstractGeometry;
import net.opengis.gml.v32.AbstractTimeGeometricPrimitive;
import net.opengis.gml.v32.Measure;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for feature resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class FeatureBindingHtml extends AbstractFeatureBindingHtml<IFeature>
{
    final IObsSystemDatabase db;
    final boolean isSummary;
    final String collectionTitle;
    boolean isHistory;
    
    
    public FeatureBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoder);
        this.db = db;
        this.isSummary = isSummary;
        
        // set collection title depending on path
        if (ctx.getParentID() != null)
        {
            if (ctx.getParentRef().type instanceof SystemHandler)
            {
                // fetch parent system name
                var parentSys = FeatureUtils.getClosestToNow(db.getSystemDescStore(), ctx.getParentID());
                this.collectionTitle = "Sampling Features of " + parentSys.getName();
            }
            else
            {
                // fetch parent feature name
                var parentFeature = FeatureUtils.getClosestToNow(db.getFoiStore(), ctx.getParentID());
                
                if (ctx.getRequestPath().contains(SystemHistoryHandler.NAMES[0]))
                {
                    this.collectionTitle = "History of " + parentFeature.getName();
                    this.isHistory = true;
                }
                else
                    this.collectionTitle = "Members of " + parentFeature.getName();
            }
        }
        else
            this.collectionTitle = "All Features of Interest";
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    public void serialize(FeatureKey key, IFeature f, boolean showLinks) throws IOException
    {
        if (isSummary)
        {
            if (isCollection)
                serializeSummary(key, f);
            else
                serializeSingleSummary(key, f);
        }
        else
            serializeDetails(key, f);
    }
    
    
    protected void serializeSingleSummary(FeatureKey key, IFeature f) throws IOException
    {
        writeHeader();
        serializeSummary(key, f);
        writeFooter();
        writer.flush();
    }
    
    
    @Override
    protected String getResourceUrl(FeatureKey key)
    {
        var foiId = encodeID(key.getInternalID());
        return ctx.getApiRootURL() + "/" + FoiHandler.NAMES[0] + "/" + foiId;
    }
    
    
    protected void serializeSummary(FeatureKey key, IFeature f) throws IOException
    {
        var resourceUrl = getResourceUrl(key);
        
        var hasMembers = db.getFoiStore().countMatchingEntries(new FoiFilter.Builder()
            .withParents(key.getInternalID())
            .build()) > 0;
        
        renderCard(
            a(f.getName())
                .withHref(resourceUrl)
                .withClass("text-decoration-none"),
            iff(f.getDescription() != null, div(
                f.getDescription()
            ).withClasses(CSS_CARD_SUBTITLE)),
            div(
                span("UID: ").withClass(CSS_BOLD),
                span(f.getUniqueIdentifier())
            ).withClass("mt-2"),
            iff(f.getType() != null, div(
                span("Feature Type: ").withClass(CSS_BOLD),
                span(getFeatureTypeSuffix(f.getType())).withTitle(f.getType())
            )),
            div(
                span("Validity Period: ").withClass(CSS_BOLD),
                getTimeExtentHtml(f.getValidTime(), "Always")
            ).withClass("mt-2"),
            div(
                span("Geometry: ").withClass(CSS_BOLD),
                text(f.getGeometry() != null ?
                    f.getGeometry().toString() : "None")
            ),
            div(
                span("Properties: ").withClass(CSS_BOLD),
                div(
                    each(f.getProperties().entrySet(), prop ->
                        getPropertyHtml(f, prop))
                    ).withClass("ps-4")
            ).withClass("mt-2"),
            p(
                iff(hasMembers,
                    a("Members").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(isHistory,
                    a("History").withHref(resourceUrl + "/history").withClasses(CSS_LINK_BTN_CLASSES))
            ).withClass("mt-4"));
    }
    
    
    protected void serializeDetails(FeatureKey key, IFeature res) throws IOException
    {
        writeHeader();
        
        h3(res.getName()).render(html);
        
        serializeSummary(key, res);
        
        writeFooter();
        writer.flush();
    }
    
    
    String getFeatureTypeSuffix(String url)
    {
        int startIdx = url.lastIndexOf('#');
        if (startIdx < 0)
            startIdx = url.lastIndexOf('/');
        return url.substring(startIdx+1);
    }
    
    
    DomContent getPropertyHtml(IFeature f, Entry<QName, Object> prop)
    {
        var propName = prop.getKey().getLocalPart();
        var val = prop.getValue();
        var featureType = f.getType();
        
        Tag<?> valueTag;
        if (val instanceof Boolean)
        {
            valueTag = span(val.toString());
        }
        else if (val instanceof Number)
        {
            valueTag = span(val.toString());
        }
        else if (val instanceof String && !val.equals(featureType))
        {
            valueTag = span(val.toString());
        }
        else if (val instanceof IXlinkReference)
        {
            String href = ((IXlinkReference<?>) val).getHref();
            if (href != null)
            {
                if (!href.equals(featureType)) 
                    valueTag = span(href);
                else
                    return new UnescapedText("");
            }
            else
                valueTag = span("Unspecified");
        }
        else if (val instanceof Measure)
        {
            var m = (Measure)val;
            valueTag = span(m.getValue() + (m.getUom() != null ? " " + m.getUom() : ""));
        }
        else if (val instanceof AbstractGeometry && val != f.getGeometry())
        {
            valueTag = span(val.toString());
        }
        else if (val instanceof AbstractTimeGeometricPrimitive &&
            !GenericTemporalFeatureImpl.PROP_VALID_TIME.getLocalPart().equals(propName))
        {
            var te = GMLUtils.timePrimitiveToTimeExtent((AbstractTimeGeometricPrimitive)val);
            valueTag = getTimeExtentHtml(te, "NA");
        }
        else
            return new UnescapedText("");
            
        return div(
            span(getPrettyName(propName) + ": ")
                .attr("title", prop.getKey())
                .withClass(CSS_BOLD),
            valueTag
        ).withClass(CSS_CARD_TEXT);
    }
    
    
    public String getPrettyName(String text)
    {
        StringBuilder buf = new StringBuilder(text.substring(text.lastIndexOf('.')+1));
        for (int i=0; i<buf.length()-1; i++)
        {
            char c = buf.charAt(i);
            
            if (i == 0)
            {
                char newcar = Character.toUpperCase(c);
                buf.setCharAt(i, newcar);
            }
                    
            else if (Character.isUpperCase(c) && Character.isLowerCase(buf.charAt(i+1)))
            {
                buf.insert(i, ' ');
                i++;
            }
        }
        
        return buf.toString();
    }
}
