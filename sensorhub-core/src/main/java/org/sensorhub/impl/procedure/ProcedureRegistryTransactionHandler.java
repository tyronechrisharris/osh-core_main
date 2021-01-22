/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure;

import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.event.IEventBus;


public class ProcedureRegistryTransactionHandler extends ProcedureObsTransactionHandler
{

    public ProcedureRegistryTransactionHandler(IEventBus eventBus, IProcedureObsDatabase db)
    {
        super(eventBus, db);
    }
    
    
    protected ProcedureDriverTransactionHandler createProcedureHandler(FeatureKey procKey, String procUID)
    {
        return new ProcedureDriverTransactionHandler(procKey, procUID, null, this);
    }

}
