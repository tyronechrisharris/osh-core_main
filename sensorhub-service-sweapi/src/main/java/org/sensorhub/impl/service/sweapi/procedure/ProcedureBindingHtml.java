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
import org.sensorhub.api.database.IProcedureDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
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
public class ProcedureBindingHtml extends SmlFeatureBindingHtml<IProcedureWithDesc>
{
    final IProcedureDatabase db;
    final String collectionTitle;
    
    
    public ProcedureBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, String collectionTitle, IProcedureDatabase db) throws IOException
    {
        super(ctx, idEncoder, isSummary);
        this.db = db;
        
        if (ctx.getParentID() != 0L)
        {
            // fetch parent system name
            var parentSys = db.getProcedureStore().getCurrentVersion(ctx.getParentID());
            this.collectionTitle = collectionTitle.replace("{}", parentSys.getName());
        }
        else
            this.collectionTitle = collectionTitle;
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    protected String getResourceUrl(FeatureKey key)
    {
        var id = Long.toString(encodeID(key.getInternalID()), ResourceBinding.ID_RADIX);
        var requestUrl = ctx.getRequestUrl();
        //var resourceUrl = isCollection ? requestUrl + "/" + sysId : requestUrl;
        var resourceUrl = isCollection ? ctx.getApiRootURL() + "/" + ProcedureHandler.NAMES[0] + "/" + id : requestUrl;
        return resourceUrl;
    }
    
    
    @Override
    protected DomContent getLinks(long id, String resourceUrl)
    {
        var hasSubSystems = db.getProcedureStore().countMatchingEntries(new ProcedureFilter.Builder()
            .withParents(id)
            .withCurrentVersion()
            .build()) > 0;
        
        return div(
            a("Spec Sheet").withHref(resourceUrl + "/details").withClasses(CSS_LINK_BTN_CLASSES),
            iff(hasSubSystems,
                a("Subsystems").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES))
        ).withClass("mt-4");
    }
}
