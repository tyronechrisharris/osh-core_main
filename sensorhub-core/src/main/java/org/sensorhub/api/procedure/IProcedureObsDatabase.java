/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.procedure;

import org.sensorhub.api.datastore.IDatabase;
import org.sensorhub.api.obs.IFoiStore;
import org.sensorhub.api.obs.IObsStore;


/**
 * <p>
 * Main interface for accessing historical observations, the description of
 * procedures that generated them, and sampling features.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 17, 2019
 */
public interface IProcedureObsDatabase extends IDatabase
{

    /**
     * @return Data store containing history of procedure descriptions
     */
    IProcedureDescStore getProcedureStore();
    
    
    /**
     * @return Data store containing features of interest observed by
     * all registered procedures
     */
    IFoiStore getFoiStore();    
    
    
    /**
     * @return Data store containing historical observations generated
     * from registered procedures
     */
    IObsStore getObservationStore();
    
}
