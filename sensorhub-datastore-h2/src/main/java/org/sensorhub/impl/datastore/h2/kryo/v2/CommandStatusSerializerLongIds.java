/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2.kryo.v2;

import static com.esotericsoftware.minlog.Log.TRACE;
import org.sensorhub.api.command.CommandStatus;
import org.sensorhub.impl.serialization.kryo.BackwardCompatFieldSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;


/**
 * <p>
 * Custom serializer for backward compatibility with v1 format where
 * command ID was serialized as BigInteger
 * </p>
 *
 * @author Alex Robin
 * @since May 3, 2022
 */
public class CommandStatusSerializerLongIds extends BackwardCompatFieldSerializer<CommandStatus>
{
    int idScope;
    
    
    public CommandStatusSerializerLongIds(Kryo kryo, int idScope)
    {
        super(kryo, CommandStatus.class);
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
                newField = new BigIdAsBigIntCachedField(f.getField(), getKryo(), idScope);
            }
            
            compatFields[i++] = newField;
        }
    }
    
    
    public void write (Kryo kryo, Output output, CommandStatus object) {
        int pop = pushTypeVariables();

        CachedField[] fields = compatFields;
        for (int i = 0, n = fields.length; i < n; i++) {
            if (TRACE) log("Write", fields[i], output.position());
            try {
                fields[i].write(output, object);
            } catch (KryoException e) {
                throw e;
            } catch (OutOfMemoryError | Exception e) {
                throw new KryoException("Error writing " + fields[i] + " at position " + output.position(), e);
            }
        }

        popTypeVariables(pop);
    }

}
