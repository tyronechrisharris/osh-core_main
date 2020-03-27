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

import org.sensorhub.api.datastore.IFeatureStore.FeatureField;
import org.sensorhub.api.datastore.IProcedureStore;
import org.sensorhub.api.procedure.IProcedureDescStore.ProcedureField;
import net.opengis.sensorml.v20.AbstractProcess;


public interface IProcedureDescStore extends IProcedureStore<AbstractProcess, ProcedureField>
{

    public static class ProcedureField extends FeatureField
    {
        public static final ProcedureField GENERAL_METADATA = new ProcedureField("metadata");
        public static final ProcedureField HISTORY = new ProcedureField("history");
        public static final ProcedureField MEMBERS = new ProcedureField("members");
        
        public ProcedureField(String name)
        {
            super(name);
        }
    }
    
}
