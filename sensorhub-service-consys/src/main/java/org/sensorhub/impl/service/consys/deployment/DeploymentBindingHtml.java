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

import java.io.IOException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.deployment.DeploymentFilter;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.system.IDeploymentWithDesc;
import org.sensorhub.impl.service.consys.resource.RequestContext;
import org.sensorhub.impl.service.consys.sensorml.SmlFeatureBindingHtml;
import j2html.tags.specialized.DivTag;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for procedure resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class DeploymentBindingHtml extends SmlFeatureBindingHtml<IDeploymentWithDesc, IObsSystemDatabase>
{
    final String collectionTitle;
    
    
    public DeploymentBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoders, isSummary, db, true);
        this.collectionTitle = "System Deployments";
    }
    
    
    @Override
    protected String getResourceName()
    {
        return "Procedure";
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
        var hasSubSystems = db.getDeploymentStore().countMatchingEntries(new DeploymentFilter.Builder()
            .withCurrentVersion()
            .build()) > 0;
        
        return div(
            //a("Details").withHref(resourceUrl).withClasses(CSS_LINK_BTN_CLASSES),
            iff(hasSubSystems,
                a("Deployed Systems").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES))
        );
    }
}
