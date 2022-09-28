/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.serialization.kryo.v2;

import org.sensorhub.api.command.CommandStatus;
import org.sensorhub.impl.serialization.kryo.BackwardCompatFieldSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializerFactory;
import com.esotericsoftware.kryo.SerializerFactory.BaseSerializerFactory;


/**
 * <p>
 * Custom serializer to write commandID as byte[]
 * </p>
 *
 * @author Alex Robin
 * @since Aug 23, 2022
 */
public class CommandStatusSerializerBigIds extends BackwardCompatFieldSerializer<CommandStatus>
{
    int idScope;
    
    
    public CommandStatusSerializerBigIds(Kryo kryo, int idScope)
    {
        super(kryo, CommandStatus.class);
        this.idScope = idScope;
        customizeCacheFields();
    }
    
    
    public CommandStatusSerializerBigIds(Kryo kryo, int idScope, Class<? extends CommandStatus> clazz)
    {
        super(kryo, clazz);
        this.idScope = idScope;
        customizeCacheFields();
    }
    
    
    protected void customizeCacheFields()
    {
        CachedField[] fields = getFields();
        
        // modify fields that have changed since v1
        int i = 0;
        compatFields = new CachedField[fields.length];
        for (var f: fields)
        {
            var name = f.getName();
            CachedField newField = f;
            
            if ("commandID".equals(name))
            {
                // use transforming field to convert between BigId and BigInteger
                newField = new BigIdAsBytesCachedField(f.getField(), idScope);
            }
            
            compatFields[i++] = newField;
        }
    }
    
    
    public static SerializerFactory<CommandStatusSerializerBigIds> factory(int idScope)
    {
        return new BaseSerializerFactory<>() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            public CommandStatusSerializerBigIds newSerializer(Kryo kryo, Class type)
            {
                return new CommandStatusSerializerBigIds(kryo, idScope, type);
            }
        };
    }

}
