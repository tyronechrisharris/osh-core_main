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
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.feature.FeatureUtils;
import org.sensorhub.impl.service.sweapi.procedure.SmlFeatureBindingHtml;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for system resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class SystemBindingHtml extends SmlFeatureBindingHtml<ISystemWithDesc>
{
    final IObsSystemDatabase db;
    final String collectionTitle;
    
    
    public SystemBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoder, isSummary);
        this.db = db;
        
        // set collection title depending on path
        if (ctx.getParentID() != null)
        {
            // fetch parent system name
            var parentSys = FeatureUtils.getClosestToNow(db.getSystemDescStore(), ctx.getParentID());
            
            if (isSummary)
            {
                if (ctx.getRequestPath().contains(SystemHistoryHandler.NAMES[0]))
                    this.collectionTitle = "History of " + parentSys.getName();
                else
                    this.collectionTitle = "Subsystems of " + parentSys.getName();
            }
            else
                this.collectionTitle = "Specsheet of " + parentSys.getName();
        }
        else
            this.collectionTitle = "All Systems";
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    protected DomContent getLinks(BigId id, String resourceUrl)
    {
        var hasSubSystems = db.getSystemDescStore().countMatchingEntries(new SystemFilter.Builder()
            .withParents(id)
            .withCurrentVersion()
            .build()) > 0;
            
        var hasFois = db.getFoiStore().countMatchingEntries(new FoiFilter.Builder()
            .withParents()
                .withInternalIDs(id)
                .includeMembers(true)
                .done()
            .includeMembers(true)
            .withCurrentVersion()
            .build()) > 0;
            
        var hasDataStreams = db.getDataStreamStore().countMatchingEntries(new DataStreamFilter.Builder()
            .withSystems()
                .withInternalIDs(id)
                .includeMembers(true)
                .done()
            .withCurrentVersion()
            .build()) > 0;
            
        var hasControls = db.getCommandStreamStore().countMatchingEntries(new CommandStreamFilter.Builder()
            .withSystems()
                .withInternalIDs(id)
                .includeMembers(true)
                .done()
            .withCurrentVersion()
            .build()) > 0;
        
        return div(
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
        ).withClass("mt-4");
    }
}
