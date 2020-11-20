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

import java.util.Collection;
import org.sensorhub.api.database.IProcedureObsDatabase;


/**
 * <p>
 * Extension to procedure/obs database interface allowing processing of
 * procedure and data events and persisting the corresponding payload.
 * </p>
 *
 * @author Alex Robin
 * @date Nov 16, 2020
 */
public interface IProcedureEventHandlerDatabase extends IProcedureObsDatabase
{
        
    Collection<String> getHandledProcedures();
    
    
    boolean isProcessEvents();
    
}
