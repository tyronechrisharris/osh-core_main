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
import org.sensorhub.api.database.IObsSystemDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.impl.service.sweapi.IdEncoder;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.vast.ogc.gml.IFeature;
import j2html.tags.DomContent;
import static j2html.TagCreator.*;


/**
 * <p>
 * HTML formatter for feature resources
 * </p>
 *
 * @author Alex Robin
 * @since March 31, 2022
 */
public class FeatureBindingHtml extends AbstractFeatureBindingHtml<IFeature, IObsSystemDatabase>
{
    final String collectionTitle;
    
    
    public FeatureBindingHtml(RequestContext ctx, IdEncoder idEncoder, boolean isSummary, IObsSystemDatabase db) throws IOException
    {
        super(ctx, idEncoder, isSummary, db);
        
        // set collection title depending on path
        if (ctx.getParentID() != null)
        {
            if (ctx.getParentRef().type instanceof SystemHandler)
            {
                // fetch parent system name
                var parentSys = FeatureUtils.getClosestToNow(db.getSystemDescStore(), ctx.getParentID());
                this.collectionTitle = "Features observed by " + parentSys.getName();
            }
            else
            {
                // fetch parent feature name
                var parentFeature = FeatureUtils.getClosestToNow(db.getFoiStore(), ctx.getParentID());
                
                if (isHistory)
                    this.collectionTitle = "History of " + parentFeature.getName();
                else
                    this.collectionTitle = "Members of " + parentFeature.getName();
            }
        }
        else
            this.collectionTitle = "All Features of Interest";
    }
    
    
    @Override
    protected String getResourceName()
    {
        return "Feature";
    }
    
    
    @Override
    protected String getCollectionTitle()
    {
        return collectionTitle;
    }
    
    
    @Override
    protected DomContent getLinks(String resourceUrl, FeatureKey key)
    {
        /*var hasMembers = db.getFoiStore().countMatchingEntries(new FoiFilter.Builder()
            .withParents(key.getInternalID())
            .withLimit(1)
            .build()) > 0;*/
        var hasMembers = false;
        
        var hasHistory = !isHistory && db.getFoiStore().countMatchingEntries(new FoiFilter.Builder()
            .withInternalIDs(key.getInternalID())
            .withAllVersions()
            .withLimit(2)
            .build()) > 1;
            
        return div(
            iff(hasMembers,
                a("Members").withHref(resourceUrl + "/members").withClasses(CSS_LINK_BTN_CLASSES)),
            iff(hasHistory,
                a("History").withHref(resourceUrl + "/history").withClasses(CSS_LINK_BTN_CLASSES))
        ).withClass("mt-4");
    }


    @Override
    protected void serializeDetails(FeatureKey key, IFeature res) throws IOException
    {
        // do nothing since simple features don't have a details page
    }
}
