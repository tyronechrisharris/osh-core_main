/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.Instant;


/**
 * <p>
 * H2 DataType implementation for FoiPeriod objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
public class FoiPeriodDataType extends KryoDataType
{
    FoiPeriodDataType()
    {
        this.configurator = kryo -> {
            
            // pre-register known types with Kryo
            kryo.register(Instant.class, 20);
        };
    }
}