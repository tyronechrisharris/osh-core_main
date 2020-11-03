/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.view;

import org.sensorhub.api.obs.IObsStore;
import org.sensorhub.api.procedure.IProcedureStore;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureFilter;
import org.sensorhub.api.procedure.IProcedureStore.ProcedureField;


/**
 * <p>
 * Filtered view implemented as a wrapper to an instance of IProcedureStore
 * </p>
 *
 * @author Alex Robin
 * @date Nov 3, 2020
 */
public class ProcedureStoreView extends FeatureStoreViewBase<IProcedureWithDesc, ProcedureField, ProcedureFilter, IProcedureStore> implements IProcedureStore
{
    
    public ProcedureStoreView(IProcedureStore delegate, ProcedureFilter viewFilter)
    {
        super(delegate, viewFilter);
    }
    
    
    @Override
    public void linkTo(IObsStore obsStore)
    {
        throw new UnsupportedOperationException(READ_ONLY_ERROR_MSG);
    }
}