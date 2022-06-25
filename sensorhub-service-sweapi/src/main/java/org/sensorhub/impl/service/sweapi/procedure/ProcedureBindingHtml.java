 /***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.procedure;

import java.io.IOException;
import org.sensorhub.api.common.IdEncoders;
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for procedure resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class ProcedureBindingHtml extends SmlFeatureBindingHtml<IProcedureWithDesc, IProcedureDatabase>
{
    final String collectionTitle;
    
    
    public ProcedureBindingHtml(RequestContext ctx, IdEncoders idEncoders, boolean isSummary, String collectionTitle, IProcedureDatabase db) throws IOException
    {
        super(ctx, idEncoders, isSummary, db);
        
        if (ctx.getParentID() != null)
        {
            // fetch parent system name
            var parentSys = db.getProcedureStore().getCurrentVersion(ctx.getParentID());
            this.collectionTitle = collectionTitle.replace("{}", parentSys.getName());
        }
        else
            this.collectionTitle = collectionTitle;
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
        var procId = idEncoders.getProcedureIdEncoder().encodeID(key.getInternalID());
        return ctx.getApiRootURL() + "/" + ProcedureHandler.NAMES[0] + "/" + procId;
    }
    
    
    @Override
    protected DomContent getLinks(String resourceUrl, FeatureKey key)
    {
        var hasSubSystems = db.getProcedureStore().countMatchingEntries(new ProcedureFilter.Builder()
            .withParents(key.getInternalID())
            .withCurrentVersion()
            .build()) > 0;
        
        return div(
            a("Spec Sheet").withHref(resourceUrl + "/details").withClasses(CSS_LINK_BTN_CLASSES),
            iff(hasSubSystems,
                a("Subsystems").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES))
        ).withClass("mt-4");
    }
}
