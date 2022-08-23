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
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.xml.namespace.QName;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
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
 * Base class for all HTML feature formatters.
 * </p>
 * 
 * @param <V> Feature type
 * @param <DB> Database type
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public abstract class AbstractFeatureBindingHtml<V extends IFeature, DB extends IDatabase> extends ResourceBindingHtml<FeatureKey, V>
{
    protected AtomicBoolean showLinks = new AtomicBoolean();
    protected final DB db;
    protected final boolean isSummary;
    protected final boolean isHistory;
    
    
    public AbstractFeatureBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, DB db) throws IOException
    {
        super(ctx, idEncoders);
        this.db = db;
        this.isSummary = isSummary;
        this.isHistory = ctx.getRequestPath().contains(AbstractFeatureHistoryHandler.NAMES[0]);
    }
    
    
    protected abstract String getResourceName();
    protected abstract String getResourceUrl(FeatureKey key);
    protected abstract DomContent getLinks(String resourceUrl, FeatureKey key);
    protected abstract void serializeDetails(FeatureKey key, V res) throws IOException;
    
    
    protected DomContent getExtraHeaderContent()
    {
        // add leaflet map
        return each(
            link()
                .withRel("stylesheet")
                .withHref("https://unpkg.com/leaflet@1.7.1/dist/leaflet.css")
                .attr("integrity", "sha512-xodZBNTC5n17Xt2atTPuE1HxjVMSvLVW9ocqUKLsCC5CXdbqCmblAshOMAS6/keqq/sMZMZ19scR4PsZChSR7A==")
                .attr("crossorigin", ""),
            script()
                .withSrc("https://unpkg.com/leaflet@1.7.1/dist/leaflet.js")
                .attr("integrity", "sha512-XQoYMqMTK8LvdxXYG3nZ448hOEQiglfqkJs1NOQV44cWnUrBc8PkAOcXy20w0vlaXaVUearIOBhiXZ5V3ynxwA==")
                .attr("crossorigin", "")
        );
    }
    
    
    @Override
    protected void writeHeader() throws IOException
    {
        super.writeHeader();
        
        var jsonQueryParams = new HashMap<>(ctx.getParameterMap());
        jsonQueryParams.remove("format"); // remove format in case it's set
        jsonQueryParams.put("f", new String[] {ResourceFormat.JSON.getMimeType()});
        String geojsonUrl = ctx.getRequestUrlWithQuery(jsonQueryParams);
        
        // start 2-columns
        html.appendStartTag("div").appendAttribute("class", "row").completeTag();
        
        // add leaflet map
        div()
        .withClasses("col", "order-2")
        .with(
            div()
                .withId("map")
                .withStyle("height: 500px"),
            script(
                "var map = L.map('map').setView([0, 0], 3);\n\n" +
            
                "L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {\n"
                + "    attribution: '&copy; <a href=\"https://www.openstreetmap.org/copyright\">OpenStreetMap</a> contributors'\n"
                + "}).addTo(map);\n\n" +
                
                "fetch('" + geojsonUrl + "')\n"
                + "    .then(response => response.json())\n"
                + "    .then(data => {\n"
                + "        let fl = L.geoJSON(data.items ? data.items : data/*, {\n"
                + "            coordsToLatLng: coords => L.latLng(coords[0], coords[1])\n"
                + "        }*/)\n"
                + "        .bindPopup(function (layer) {\n"
                + "            return layer.feature.properties.name;\n"
                + "        })\n"
                + "        .addTo(map);\n\n"
                + "        map.invalidateSize();\n"
                + "        map.fitBounds(fl.getBounds().pad(0.01), { maxZoom:14 } );\n"
                + "    });\n\n"
            )
        ).render(html);
            
        // start 1st column div
        html.appendStartTag("div").appendAttribute("class", "col order-1").completeTag();
    }
    
    
    @Override
    protected void writeFooter() throws IOException
    {
        html.appendEndTag("div");
        html.appendEndTag("div");
        super.writeFooter();
    }
    
    
    @Override
    public void serialize(FeatureKey key, V f, boolean showLinks) throws IOException
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
    
    
    protected void serializeSingleSummary(FeatureKey key, V f) throws IOException
    {
        writeHeader();
        serializeSummary(key, f);
        writeFooter();
        writer.flush();
    }
    
    
    protected void serializeSummary(FeatureKey key, IFeature f) throws IOException
    {
        var resourceUrl = getResourceUrl(key);
        
        renderCard(
            a(
                span(f.getName()), 
                isHistory ? span("(" +
                  f.getValidTime().begin().truncatedTo(ChronoUnit.SECONDS).toString() +
                ")") : null
            ).withHref(resourceUrl)
             .withClass("text-decoration-none"),
            iff(Optional.ofNullable(f.getDescription()), desc -> div(desc)
                .withClasses(CSS_CARD_SUBTITLE)),
            div(
                span("UID: ").withClass(CSS_BOLD),
                span(f.getUniqueIdentifier())
            ).withClass("mt-2"),
            iff(Optional.ofNullable(f.getType()), type -> div(
                span(getResourceName() + " Type: ").withClass(CSS_BOLD),
                span(getFeatureTypeSuffix(f.getType())).withTitle(type)
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
            getLinks(resourceUrl, key)
        );
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
