/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.serialization.kryo.v3;

import org.sensorhub.api.common.BigId;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class BigIdSerializer extends Serializer<BigId>
{
    int idScope;
    
    
    public BigIdSerializer(int idScope)
    {
        this.idScope = idScope;
    }
    
    
    @Override
    public void write(Kryo kryo, Output output, BigId id)
    {
        output.writeVarLong(id.getIdAsLong(), true);
    }


    @Override
    public BigId read(Kryo kryo, Input input, Class<? extends BigId> type)
    {
        var id = input.readVarLong(true);
        return BigId.fromLong(idScope, id);
    }

}
