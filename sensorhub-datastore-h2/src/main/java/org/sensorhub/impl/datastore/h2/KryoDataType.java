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

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.sensorhub.impl.datastore.h2.index.PersistentClassResolver;
import org.sensorhub.impl.serialization.kryo.VersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.SerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import com.google.common.collect.ImmutableMap;


public class KryoDataType implements DataType
{
    static Logger log = LoggerFactory.getLogger(KryoDataType.class);
    
    //final Pool<KryoInstance> kryoPool;
    final ThreadLocal<KryoInstance> kryoLocal;
    protected ClassResolver classResolver;
    protected Consumer<Kryo> configurator;
    protected SerializerFactory<?> defaultObjectSerializer;
    protected int averageRecordSize, maxRecordSize;
    
    
    static class KryoInstance
    {
        Kryo kryo;
        Output output;
        Input input;
        
        KryoInstance(SerializerFactory<?> defaultObjectSerializer, ClassResolver classResolver, Consumer<Kryo> configurator, int bufferSize, int maxBufferSize)
        {
            kryo = classResolver != null ? new Kryo(classResolver, null) : new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setReferences(true);
            
            // instantiate classes using default (private) constructor when available
            // or using direct JVM technique when needed
            kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            
            // set default object serializer
            kryo.setDefaultSerializer(defaultObjectSerializer);
            
            // configure kryo instance
            if (configurator != null)
                configurator.accept(kryo);
            
            // load persistent class mappings if needed
            if (classResolver instanceof PersistentClassResolver)
                ((PersistentClassResolver) classResolver).loadMappings();
            
            input = new Input();
            output = new Output(bufferSize, maxBufferSize);
        }
    }
    
    
    public KryoDataType()
    {
        this(10*1024*1024);
    }
    
    
    public KryoDataType(final int maxRecordSize)
    {
        //Log.set(Log.LEVEL_TRACE);
        this.maxRecordSize = maxRecordSize;
        
        // set default serializer to our versioned serializer
        this.defaultObjectSerializer = VersionedSerializer.<Object>factory2(ImmutableMap.of(
            MVObsDatabase.CURRENT_VERSION, new SerializerFactory.FieldSerializerFactory()),
            MVObsDatabase.CURRENT_VERSION);
        
        /*// use both a pool and thread local
        // we get the object from thread local everytime
        this.kryoPool = new Pool<KryoInstance>(true, false, 1024) {
            @Override
            protected KryoInstance create()
            {
                return new KryoInstance(defaultObjectSerializer, classResolver, configurator, 2*averageRecordSize, maxRecordSize);
            }
        };
        
        this.kryoLocal = new ThreadLocal<KryoInstance>()
        {
            public KryoInstance initialValue()
            {
                return kryoPool.obtain();
            }
            
            public void remove()
            {
                kryoPool.free(this.get());
                super.remove();
            }
        };*/
        
        this.kryoLocal = new ThreadLocal<KryoInstance>()
        {
            public KryoInstance initialValue()
            {
                log.debug("Loading Kryo instance for " + KryoDataType.this.getClass().getSimpleName());
                return new KryoInstance(defaultObjectSerializer, classResolver, configurator, 2*averageRecordSize, maxRecordSize);
            }
        };
    }
    
    
    @Override
    public int compare(Object a, Object b)
    {
        // don't care since we won't use this for keys
        return 0;
    }


    @Override
    public int getMemory(Object obj)
    {
        initRecordSize(obj);
        return averageRecordSize;
    }


    @Override
    public void write(WriteBuffer buff, Object obj)
    {
        initRecordSize(obj);
        
        KryoInstance kryoI = kryoLocal.get();
        Kryo kryo = kryoI.kryo;
        Output output = kryoI.output;
        output.setPosition(0);
        
        //kryo.writeObjectOrNull(output, obj, objectType);
        kryo.writeClassAndObject(output, obj);
        buff.put(output.getBuffer(), 0, output.position());
        
        // adjust the average size using an exponential moving average
        int size = output.position();
        averageRecordSize = (size + averageRecordSize*4) / 5;
    }
    
    
    protected void initRecordSize(Object obj)
    {
        if (averageRecordSize <= 0)
        {
            KryoInstance kryoI = kryoLocal.get();
            Kryo kryo = kryoI.kryo;
            Output output = kryoI.output;
            output.setPosition(0);
            kryo.writeClassAndObject(output, obj);
            averageRecordSize = output.position();
            output.setBuffer(new byte[averageRecordSize*2], maxRecordSize);
        }
    }


    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i = 0; i < len; i++)
            write(buff, obj[i]);
    }


    @Override
    public Object read(ByteBuffer buff)
    {
        KryoInstance kryoI = kryoLocal.get();
        Kryo kryo = kryoI.kryo;
        Input input = kryoI.input;
        
        input.setBuffer(buff.array(), buff.position(), buff.remaining());
        //Object obj = kryo.readObjectOrNull(input, objectType);
        Object obj = kryo.readClassAndObject(input);
        buff.position(input.position());
        
        return obj;
    }


    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i = 0; i < len; i++)
            obj[i] = read(buff);
    }

}
