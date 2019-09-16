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

import org.vast.ogc.om.IProcedure;


/**
 * <p>
 * Generic interface for all procedure description stores
 * </p>
 *
 * @author Alex Robin
 * @param <T> type of procedure
 * @date Mar 19, 2018
 */
public interface IProcedureStore<T extends IProcedure> extends IFeatureStore<FeatureKey, T>
{
    
    /**
     * Link this store to an observation store to enable JOIN queries
     * @param obsStore
     */
    public void linkTo(IObsStore obsStore);

}
