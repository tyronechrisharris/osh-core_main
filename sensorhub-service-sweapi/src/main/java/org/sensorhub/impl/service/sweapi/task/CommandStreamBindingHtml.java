/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.task;

import java.io.IOException;
import org.sensorhub.api.command.ICommandStreamInfo;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.CommandStreamKey;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.SWECommonUtils;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBindingHtml;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import com.google.common.collect.ImmutableList;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for command stream resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class CommandStreamBindingHtml extends ResourceBindingHtml<CommandStreamKey, ICommandStreamInfo>
{
    final boolean isSummary;
    final String collectionTitle;
    
    
    public CommandStreamBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, String collectionTitle, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoder);
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
    
    
    public CommandStreamBindingHtml(RequestContext ctx, IdEncoder idEncoder) throws IOException
    {
        super(ctx, idEncoder);
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
    protected String getResourceUrl(CommandStreamKey key)
    {
        var dsId = encodeID(key.getInternalID());
        var requestUrl = ctx.getRequestUrl();
        return isCollection ? requestUrl + "/" + dsId : requestUrl;
    }

    
    @Override
    public void serialize(CommandStreamKey key, ICommandStreamInfo dsInfo, boolean showLinks) throws IOException
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
    
    
    protected void serializeSummary(CommandStreamKey key, ICommandStreamInfo dsInfo) throws IOException
    {
        var resourceUrl = getResourceUrl(key);
        
        var sysId = encodeID(dsInfo.getSystemID().getInternalID());
        var sysUrl = ctx.getApiRootURL() + "/" + SystemHandler.NAMES[0] + "/" + sysId;
        
        renderCard(
            a(dsInfo.getName())
                .withHref(resourceUrl)
                .withClass("text-decoration-none"),
            iff(dsInfo.getDescription() != null, div(
                dsInfo.getDescription()
            ).withClasses(CSS_CARD_SUBTITLE)),
            div(
                span("Receiving System: ").withClass(CSS_BOLD),
                a(dsInfo.getSystemID().getUniqueID()).withHref(sysUrl)
            ),
            div(
                span("Control Input: ").withClass(CSS_BOLD),
                span(dsInfo.getControlInputName())
            ).withClass("mb-2"),
            div(
                span("Valid Time: ").withClass(CSS_BOLD),
                getTimeExtentHtml(dsInfo.getValidTime(), "Always")
            ).withClass("mb-2"),
            div(
                span("Issue Time: ").withClass(CSS_BOLD),
                getTimeExtentHtml(dsInfo.getIssueTimeRange(), "No Data")
            ).withClass("mb-2"),
            div(
                span("Execution Time: ").withClass(CSS_BOLD),
                getTimeExtentHtml(dsInfo.getExecutionTimeRange(), "No Data")
            ).withClass("mb-2"),
            iffElse(isCollection,
                div(
                    span("Controllable Properties: ").withClass(CSS_BOLD),
                    div(
                        each(ImmutableList.copyOf(SWECommonUtils.getProperties(dsInfo.getRecordStructure())),
                            comp -> span(getComponentLabel(comp) + ", "))
                    ).withClass("ps-4")
                ).withClass("mb-2"),
                div(
                    span("Controllable Properties: ").withClass(CSS_BOLD),
                    div(
                        each(ImmutableList.copyOf(SWECommonUtils.getProperties(dsInfo.getRecordStructure())),
                            comp -> getPropertyHtml(comp))
                    ).withClass("ps-4")
                ).withClass("mb-2")
            ),
            p(
                iff(isCollection,
                    a("Details").withHref(resourceUrl).withClasses(CSS_LINK_BTN_CLASSES)
                ),
                a("Schema").withHref(resourceUrl + "/schema").withClasses(CSS_LINK_BTN_CLASSES),
                a("Commands").withHref(resourceUrl + "/commands").withClasses(CSS_LINK_BTN_CLASSES),
                a("Status").withHref(resourceUrl + "/status").withClasses(CSS_LINK_BTN_CLASSES)
            ).withClass("mt-4"));
    }
    
    
    protected void serializeSingleSummary(CommandStreamKey key, ICommandStreamInfo dsInfo) throws IOException
    {
        writeHeader();
        serializeSummary(key, dsInfo);
        writeFooter();
        writer.flush();
    }
    
    
    protected void serializeDetails(CommandStreamKey key, ICommandStreamInfo dsInfo) throws IOException
    {
        writeHeader();
        
        div(
            h3(dsInfo.getName()),
            h5("Command Message Structure"),
            small(
                getComponentHtml(dsInfo.getRecordStructure()),
                getEncodingHtml(dsInfo.getRecordEncoding())
            )
        ).render(html);
        
        writeFooter();
        writer.flush();
    }
}
