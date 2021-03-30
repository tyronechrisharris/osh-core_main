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

import org.vast.data.AbstractDataBlock;
import org.vast.data.DataBlockBoolean;
import org.vast.data.DataBlockByte;
import org.vast.data.DataBlockCompressed;
import org.vast.data.DataBlockDouble;
import org.vast.data.DataBlockFloat;
import org.vast.data.DataBlockInt;
import org.vast.data.DataBlockLong;
import org.vast.data.DataBlockMixed;
import org.vast.data.DataBlockParallel;
import org.vast.data.DataBlockShort;
import org.vast.data.DataBlockString;
import org.vast.data.DataBlockTuple;
import org.vast.data.DataBlockUByte;
import org.vast.data.DataBlockUInt;
import org.vast.data.DataBlockUShort;


/**
 * <p>
 * H2 DataType implementation for ObsData objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVObsResultDataType extends KryoDataType
{
    MVObsResultDataType()
    {
        // pre-register known types with Kryo
        registeredClasses.put(100, DataBlockBoolean.class);
        registeredClasses.put(101, DataBlockByte.class);
        registeredClasses.put(102, DataBlockUByte.class);
        registeredClasses.put(103, DataBlockShort.class);
        registeredClasses.put(104, DataBlockUShort.class);
        registeredClasses.put(105, DataBlockInt.class);
        registeredClasses.put(106, DataBlockUInt.class);
        registeredClasses.put(107, DataBlockLong.class);
        registeredClasses.put(108, DataBlockFloat.class);
        registeredClasses.put(109, DataBlockDouble.class);
        registeredClasses.put(110, DataBlockString.class);
        registeredClasses.put(111, AbstractDataBlock[].class);
        registeredClasses.put(112, DataBlockTuple.class);
        registeredClasses.put(113, DataBlockParallel.class);
        registeredClasses.put(114, DataBlockMixed.class);
        registeredClasses.put(115, DataBlockCompressed.class);
    }
}