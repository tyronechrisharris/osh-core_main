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

import org.h2.mvstore.MVMap;
import org.sensorhub.api.command.CommandData;
import org.sensorhub.impl.datastore.h2.kryo.KryoDataType;
import org.sensorhub.impl.datastore.h2.kryo.PersistentClassResolver;
import org.sensorhub.impl.datastore.h2.kryo.v2.CommandDataSerializerLongIds;
import org.sensorhub.impl.serialization.kryo.VersionedSerializer;
import com.esotericsoftware.kryo.Serializer;
import com.google.common.collect.ImmutableMap;


/**
 * <p>
 * H2 DataType implementation for CommandData objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class CommandDataType extends KryoDataType
{
    CommandDataType(MVMap<String, Integer> kryoClassMap, int idScope)
    {
        this.classResolver = () -> new PersistentClassResolver(kryoClassMap);
        this.configurator = kryo -> {
            // register custom serializers
            
            // configure compatibility serializer
            kryo.addDefaultSerializer(CommandData.class, new VersionedSerializer<CommandData>(
                ImmutableMap.<Integer, Serializer<CommandData>>builder()
                    .put(1, new CommandDataSerializerLongIds(kryo, idScope))
                    //.put(2, new CommandDataSerializer(kryo, idScope))
                    .build(), 1));
        };
    }
}