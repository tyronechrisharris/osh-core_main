/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.view;

import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.obs.ObsFilter;


/**
 * <p>
 * Configuration class used to define a new filtered view or reference an 
 * existing one.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 1, 2020
 */
public class ProcedureObsDatabaseViewConfig
{
    public String filteredViewId;
    
    public String sourceDatabaseId; // can be itself a filter view?
    
    public ObsFilter includeFilter;
    
    public ObsFilter excludeFilter;
    
        
    public IProcedureObsDatabase getFilteredView(ISensorHub hub)
    {
        var srcDatabase = sourceDatabaseId != null ?
            hub.getDatabaseRegistry().getDatabase(sourceDatabaseId) :
            hub.getDatabaseRegistry().getFederatedObsDatabase();
        
        if (includeFilter != null)
            return new ProcedureObsDatabaseView(srcDatabase, includeFilter);
        else
            return srcDatabase;
    }
           
}
