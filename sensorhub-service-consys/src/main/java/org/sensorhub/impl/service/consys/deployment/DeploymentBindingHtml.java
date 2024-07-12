 /***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.deployment;

import static j2html.TagCreator.a;
import static j2html.TagCreator.div;
import static j2html.TagCreator.each;
import static j2html.TagCreator.iff;
import java.io.IOException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.deployment.DeploymentFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.IDeploymentWithDesc;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.sensorml.SmlFeatureBindingHtml;
import j2html.tags.specialized.DivTag;


/**
 * <p>
 * HTML formatter for deployment resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2023
 */
public class DeploymentBindingHtml extends SmlFeatureBindingHtml<IDeploymentWithDesc, IObsSystemDatabase>
{
    final String collectionTitle;
    
    
    public DeploymentBindingHtml(RequestContext ctx, IdEncoders idEncoders, IObsSystemDatabase db, boolean isSummary) throws IOException
    {
        super(ctx, idEncoders, db, isSummary, true);
        this.collectionTitle = "System Deployments";
    }
    
    
    @Override
    protected String getResourceName()
    {
        return "Deployment";
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    protected String getResourceUrl(FeatureKey key)
    {
        var procId = idEncoders.getDeploymentIdEncoder().encodeID(key.getInternalID());
        return ctx.getApiRootURL() + "/" + DeploymentHandler.NAMES[0] + "/" + procId;
    }
    
    
    @Override
    protected DivTag getLinks(String resourceUrl, FeatureKey key, IDeploymentWithDesc f)
    {
        // try to get parent deployment
        String parentDeplUrl = null;
        var parentDeplId = db.getDeploymentStore().getParent(key.getInternalID());
        if (parentDeplId != null)
        {
            var deplId = idEncoders.getDeploymentIdEncoder().encodeID(parentDeplId);
            parentDeplUrl = ctx.getApiRootURL() + "/" + DeploymentHandler.NAMES[0] + "/" + deplId;
        }
        
        var hasSubDepl = db.getDeploymentStore().countMatchingEntries(new DeploymentFilter.Builder()
            .withParents(key.getInternalID())
            .withLimit(1)
            .build()) > 0;
            
//        var hasFois = db.getDeploymentStore().countMatchingEntries(new FoiFilter.Builder()
//            .withParents()
//                .withInternalIDs(key.getInternalID())
//                .includeMembers(true)
//                .done()
//            .includeMembers(true)
//            //.withCurrentVersion()
//            .withLimit(1)
//            .build()) > 0;
//            
//        var hasDataStreams = db.getDataStreamStore().countMatchingEntries(new DataStreamFilter.Builder()
//            .withSystems()
//                .withInternalIDs(key.getInternalID())
//                .includeMembers(true)
//                .done()
//            //.withCurrentVersion()
//            .withLimit(1)
//            .build()) > 0;
        
        return div(
            //a("Details").withHref(resourceUrl).withClasses(CSS_LINK_BTN_CLASSES),
            !isHistory ? each(
                iff(parentDeplUrl != null,
                    a("Parent Deployment").withHref(parentDeplUrl).withClasses(CSS_LINK_BTN_CLASSES)),
                iff(hasSubDepl,
                    a("Subdeployments").withHref(resourceUrl + "/" + DeploymentMembersHandler.NAMES[0]).withClasses(CSS_LINK_BTN_CLASSES))
//                iff(hasFois,
//                    a("Sampling Features").withHref(resourceUrl + "/" + FoiHandler.NAMES[0]).withClasses(CSS_LINK_BTN_CLASSES)),
//                iff(hasDataStreams,
//                    a("Datastreams").withHref(resourceUrl + "/" + DataStreamHandler.NAMES[0]).withClasses(CSS_LINK_BTN_CLASSES)),
            ) : null
        );
    }
}
