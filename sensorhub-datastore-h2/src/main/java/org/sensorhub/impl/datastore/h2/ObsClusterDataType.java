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
import org.vast.util.Bbox;
import com.google.common.collect.Range;


/**
 * <p>
 * H2 DataType implementation for ObsCluster objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class ObsClusterDataType extends KryoDataType
{
    ObsClusterDataType()
    {
        // pre-register known types with Kryo
        registeredClasses.put(20, Range.class);
        registeredClasses.put(21, Instant.class);
        registeredClasses.put(22, Bbox.class);
    }
}