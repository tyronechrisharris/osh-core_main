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
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.vast.ogc.gml.IFeature;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;


/**
 * <p>
 * Base class for all HTML feature formatters.
 * </p>
 * 
 * @param <V> Feature type
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public abstract class AbstractFeatureBindingHtml<V extends IFeature> extends ResourceBindingHtml<FeatureKey, V>
{
    protected AtomicBoolean showLinks = new AtomicBoolean();
    
    
    public AbstractFeatureBindingHtml(RequestContext ctx, IdEncoder idEncoder) throws IOException
    {
        super(ctx, idEncoder);
    }
    
    
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
    
    
    protected String getCollectionTitle()
    {
        return "Feature Collection";
    }
    
    
    @Override
    protected void writeHeader() throws IOException
    {
        super.writeHeader();
        
        var jsonQueryParams = new HashMap<>(ctx.getParameterMap());
        jsonQueryParams.remove("format"); // remove format in case it's set
        jsonQueryParams.put("f", new String[] {ResourceFormat.JSON.getMimeType()});
        String geojsonUrl = ctx.getRequestUrlWithQuery(jsonQueryParams);
        
        if (isCollection)
            h3(getCollectionTitle()).render(html);
        
        // start 2-columns
        html.appendStartTag("div").appendAttribute("class", "row").completeTag();
        
        // add leaflet map
        div()
        .withClasses("col", "order-2", "pt-4")
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
                + "        let fl = L.geoJSON(data.items ? data.items : data, {\n"
                + "            coordsToLatLng: coords => L.latLng(coords[0], coords[1])\n"
                + "        })\n"
                + "        .bindPopup(function (layer) {\n"
                + "            return layer.feature.properties.name;\n"
                + "        })\n"
                + "        .addTo(map);\n\n"
                + "        map.fitBounds(fl.getBounds(), { maxZoom:14 } );\n"
                + "        map.invalidateSize();\n"
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
    protected String getResourceUrl(FeatureKey key)
    {
        var fid = encodeID(key.getInternalID());
        var requestUrl = ctx.getRequestUrl();
        return isCollection ? requestUrl + "/" + fid : requestUrl;
    }
}
