/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.obs;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.SWECommonUtils;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.vast.util.Asserts;
import com.google.common.collect.ImmutableList;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for datastream resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class DataStreamBindingHtml extends ResourceBindingHtml<DataStreamKey, IDataStreamInfo>
{
    final Map<String, CustomObsFormat> customFormats;
    final ResourceFormat obsFormat;
    final boolean isSummary;
    final String collectionTitle;
    
    
    public DataStreamBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, String collectionTitle, IObsSystemDatabase db, Map<String, CustomObsFormat> customFormats) throws IOException
    {
        super(ctx, idEncoder);
        this.customFormats = Asserts.checkNotNull(customFormats);
        this.obsFormat = null;
        this.isSummary = isSummary;
        
        if (ctx.getParentID() != null)
        {
            // fetch parent system name
            var parentSys = db.getSystemDescStore().getCurrentVersion(ctx.getParentID());
            this.collectionTitle = collectionTitle.replace("{}", parentSys.getName());
        }
        else
            this.collectionTitle = collectionTitle;
    }
    
    
    public DataStreamBindingHtml(RequestContext ctx, IdEncoder idEncoder, ResourceFormat obsFormat) throws IOException
    {
        super(ctx, idEncoder);
        this.obsFormat = obsFormat;
        this.customFormats = null;
        this.isSummary = false;
        this.collectionTitle = null;
    }
    
    
    @Override
    protected void writeHeader() throws IOException
    {
        super.writeHeader();
        
        if (isCollection)
            h3(collectionTitle).render(html);
    }

    
    @Override
    public void serialize(DataStreamKey key, IDataStreamInfo dsInfo, boolean showLinks) throws IOException
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
    
    
    @Override
    protected String getResourceUrl(DataStreamKey key)
    {
        var dsId = encodeID(key.getInternalID());
        var requestUrl = ctx.getRequestUrl();
        return isCollection ? requestUrl + "/" + dsId : requestUrl;
    }
    
    
    protected void serializeSummary(DataStreamKey key, IDataStreamInfo dsInfo) throws IOException
    {
        var resourceUrl = getResourceUrl(key);
        
        renderCard(
            a(dsInfo.getName())
                .withHref(resourceUrl)
                .withClass("text-decoration-none"),
            iff(dsInfo.getDescription() != null, div(
                dsInfo.getDescription()
            ).withClasses(CSS_CARD_SUBTITLE)),
            div(
                span("Parent System: ").withClass(CSS_BOLD),
                span(dsInfo.getSystemID().getUniqueID())
            ),
            div(
                span("Output: ").withClass(CSS_BOLD),
                span(dsInfo.getOutputName())
            ).withClass("mb-2"),
            div(
                span("Valid Time: ").withClass(CSS_BOLD),
                getTimeExtentHtml(dsInfo.getValidTime(), "Always")
            ).withClass("mb-2"),
            div(
                span("Phenomenon Time: ").withClass(CSS_BOLD),
                getTimeExtentHtml(dsInfo.getPhenomenonTimeRange(), "No Data")
            ).withClass("mb-2"),
            div(
                span("Result Time: ").withClass(CSS_BOLD),
                getTimeExtentHtml(dsInfo.getResultTimeRange(), "No Data")
            ).withClass("mb-2"),
            iffElse(isCollection,
                div(
                    span("Observed Properties: ").withClass(CSS_BOLD),
                    div(
                        each(ImmutableList.copyOf(SWECommonUtils.getProperties(dsInfo.getRecordStructure())),
                            comp -> span(getComponentLabel(comp) + ", "))
                    ).withClass("ps-4")
                ).withClass("mb-2"),
                div(
                    span("Observed Properties: ").withClass(CSS_BOLD),
                    div(
                        each(ImmutableList.copyOf(SWECommonUtils.getProperties(dsInfo.getRecordStructure())),
                            comp -> getPropertyHtml(comp))
                    ).withClass("ps-4")
                ).withClass("mb-2")
            ),
            iff(!isCollection,
                div(
                    span("Available Observation Formats: ").withClass(CSS_BOLD),
                    div(
                        each(SWECommonUtils.getAvailableFormats(dsInfo, customFormats), f -> {
                            var fUrl = URLEncoder.encode(f, StandardCharsets.UTF_8);
                            return div(
                                span(f + ": "),
                                a("Schema").withHref(resourceUrl + "/schema?obsFormat=" + fUrl).withClass("small"),
                                span(" - "),
                                a("Observations").withHref(resourceUrl + "/observations?f=" + fUrl).withClass("small")
                            );
                        })
                    ).withClass("ps-4")
                ).withClass("mb-2")
            ),
            iff(isCollection,
                p(
                    a("Details").withHref(resourceUrl).withClasses(CSS_LINK_BTN_CLASSES),
                    a("Schema").withHref(resourceUrl + "/schema").withClasses(CSS_LINK_BTN_CLASSES),
                    a("Observations").withHref(resourceUrl + "/observations").withClasses(CSS_LINK_BTN_CLASSES)
                ).withClass("mt-4")
            )
         );
    }
    
    
    protected void serializeSingleSummary(DataStreamKey key, IDataStreamInfo dsInfo) throws IOException
    {
        writeHeader();
        serializeSummary(key, dsInfo);
        writeFooter();
        writer.flush();
    }
    
    
    protected void serializeDetails(DataStreamKey key, IDataStreamInfo dsInfo) throws IOException
    {
        writeHeader();
        
        div(
            h3(dsInfo.getName()),
            h5("Record Structure"),
            small(
                getComponentHtml(dsInfo.getRecordStructure()),
                getEncodingHtml(dsInfo.getRecordEncoding())
            )
        ).render(html);
        
        writeFooter();
        writer.flush();
    }
}
