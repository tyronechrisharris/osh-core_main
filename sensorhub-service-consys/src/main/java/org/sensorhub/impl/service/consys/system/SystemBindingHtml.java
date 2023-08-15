/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.system;

import java.io.IOException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.api.system.ISystemWithDesc;
import org.sensorhub.impl.service.consys.feature.FeatureUtils;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.sensorml.SmlFeatureBindingHtml;
import j2html.tags.specialized.DivTag;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for system resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class SystemBindingHtml extends SmlFeatureBindingHtml<ISystemWithDesc, IObsSystemDatabase>
{
    final String collectionTitle;
    
    
    public SystemBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoders, isSummary, db, true);
        
        // set collection title depending on path
        if (ctx.getParentID() != null)
        {
            // fetch parent system name
            var parentSys = FeatureUtils.getClosestToNow(db.getSystemDescStore(), ctx.getParentID());
            
            if (isHistory)
                this.collectionTitle = "History of " + parentSys.getName();
            else
                this.collectionTitle = "Subsystems of " + parentSys.getName();
        }
        else
            this.collectionTitle = "System Instances";
    }
    
    
    @Override
    protected String getResourceName()
    {
        return "System";
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    protected String getResourceUrl(FeatureKey key)
    {
        var sysId = idEncoders.getSystemIdEncoder().encodeID(key.getInternalID());
        var sysUrl = ctx.getApiRootURL() + "/" + SystemHandler.NAMES[0] + "/" + sysId;
        if (isHistory)
            sysUrl += "/history/" + key.getValidStartTime().toString();
        return sysUrl;
    }
    
    
    @Override
    protected DivTag getLinks(String resourceUrl, FeatureKey key)
    {
        // try to get parent system
        String parentSysUrl = null;
        var parentSysId = db.getSystemDescStore().getParent(key.getInternalID());
        if (parentSysId != null)
        {
            var sysId = idEncoders.getSystemIdEncoder().encodeID(parentSysId);
            parentSysUrl = ctx.getApiRootURL() + "/" + SystemHandler.NAMES[0] + "/" + sysId;
        }
        
        var hasSubSystems = db.getSystemDescStore().countMatchingEntries(new SystemFilter.Builder()
            .withParents(key.getInternalID())
            .withCurrentVersion()
            .withLimit(1)
            .build()) > 0;
            
        var hasFois = db.getFoiStore().countMatchingEntries(new FoiFilter.Builder()
            .withParents()
                .withInternalIDs(key.getInternalID())
                .includeMembers(true)
                .done()
            .includeMembers(true)
            //.withCurrentVersion()
            .withLimit(1)
            .build()) > 0;
            
        var hasDataStreams = db.getDataStreamStore().countMatchingEntries(new DataStreamFilter.Builder()
            .withSystems()
                .withInternalIDs(key.getInternalID())
                .includeMembers(true)
                .done()
            //.withCurrentVersion()
            .withLimit(1)
            .build()) > 0;
            
        var hasControls = db.getCommandStreamStore().countMatchingEntries(new CommandStreamFilter.Builder()
            .withSystems()
                .withInternalIDs(key.getInternalID())
                .includeMembers(true)
                .done()
            .withCurrentVersion()
            .withLimit(1)
            .build()) > 0;
            
        var hasHistory = !isHistory && db.getSystemDescStore().countMatchingEntries(new SystemFilter.Builder()
            .withInternalIDs(key.getInternalID())
            .withAllVersions()
            .withLimit(2)
            .build()) > 1;
        
        return div(
            //a("Details").withHref(resourceUrl).withClasses(CSS_LINK_BTN_CLASSES),
            !isHistory ? each(
                iff(parentSysUrl != null,
                    a("Parent System").withHref(parentSysUrl).withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasSubSystems,
                    a("Subsystems").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasFois,
                    a("Sampling Features").withHref(resourceUrl + "/fois").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasDataStreams,
                    a("Datastreams").withHref(resourceUrl + "/datastreams").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasControls,
                    a("Control Channels").withHref(resourceUrl + "/controls").withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasHistory,
                    a("History").withHref(resourceUrl + "/history").withClasses(CSS_LINK_BTN_CLASSES))
            ) : null
        );
    }
}
