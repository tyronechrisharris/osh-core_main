/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.kryo;

import java.util.Map;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class VersionedSerializer<T> extends Serializer<T>
{
    final Map<Integer, Serializer<T>> serializers;
    final int writeVersion;
    final Serializer<T> writer;
    
    
    public VersionedSerializer(Map<Integer, Serializer<T>> serializers, int writeVersion)
    {
        this.serializers = serializers;
        this.writeVersion = writeVersion;
        this.writer = serializers.get(writeVersion);
    }
    
    
    @Override
    public void write(Kryo kryo, Output output, T object)
    {
        output.writeVarInt(writeVersion, true);
        writer.write(kryo, output, object);        
    }
    

    @Override
    public T read(Kryo kryo, Input input, Class<T> type)
    {
        int version = input.readVarInt(true);
        var reader = serializers.get(version);
        if (reader == null)
            throw new IllegalStateException("Unsupported version " + version);
        return reader.read(kryo, input, type);
    }

}
