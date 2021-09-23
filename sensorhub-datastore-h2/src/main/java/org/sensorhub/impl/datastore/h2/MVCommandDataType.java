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
import org.sensorhub.api.command.CommandData;
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
 * H2 DataType implementation for CommandData objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVCommandDataType extends KryoDataType
{
    MVCommandDataType()
    {
        this.configurator = kryo -> {
            
            // pre-register known types with Kryo
            kryo.register(CommandData.class, 20);
            kryo.register(Instant.class, 30);
            kryo.register(DataBlockBoolean.class, 100);
            kryo.register(DataBlockByte.class, 101);
            kryo.register(DataBlockUByte.class, 102);
            kryo.register(DataBlockShort.class, 103);
            kryo.register(DataBlockUShort.class, 104);
            kryo.register(DataBlockInt.class, 105);
            kryo.register(DataBlockUInt.class, 106);
            kryo.register(DataBlockLong.class, 107);
            kryo.register(DataBlockFloat.class, 108);
            kryo.register(DataBlockDouble.class, 109);
            kryo.register(DataBlockString.class, 110);
            kryo.register(AbstractDataBlock[].class, 111);
            kryo.register(DataBlockTuple.class, 112);
            kryo.register(DataBlockParallel.class, 113);
            kryo.register(DataBlockMixed.class, 114);
            kryo.register(DataBlockCompressed.class, 115);
        };
    }
}