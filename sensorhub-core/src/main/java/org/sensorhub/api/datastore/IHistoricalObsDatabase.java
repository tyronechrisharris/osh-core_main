/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import org.sensorhub.api.procedure.IProcedureDescriptionStore;


/**
 * <p>
 * Main interface for accessing historical observations and corresponding
 * procedure descriptions history.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 17, 2019
 */
public interface IHistoricalObsDatabase extends IDatabase
{

    /**
     * @return Data store containing history of procedure descriptions
     */
    IProcedureDescriptionStore getProcedureStore();
    
    
    
    /**
     * @return Data store containing historical observations generated
     * from registered procedures
     */
    IObsStore getObservationStore();
    
    
    /**
     * @return Data store containing features of interest observed by
     * all registered procedures
     */
    IFoiStore getFoiStore();
    
}
