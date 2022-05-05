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
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.h2.kryo.KryoDataType;
import org.sensorhub.impl.datastore.h2.kryo.PersistentClassResolver;
import org.sensorhub.impl.datastore.h2.kryo.v2.FeatureIdSerializerLongIds;
import org.sensorhub.impl.serialization.kryo.VersionedSerializer;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.google.common.collect.ImmutableMap;
import net.opengis.OgcPropertyList;


class DataStreamInfoDataType extends KryoDataType
{
    DataStreamInfoDataType(MVMap<String, Integer> kryoClassMap, int idScope)
    {
        this.classResolver = () -> new PersistentClassResolver(kryoClassMap);
        this.configurator = kryo -> {
            
            // avoid using collection serializer on OgcPropertyList because
            // the add method doesn't behave as expected
            kryo.addDefaultSerializer(OgcPropertyList.class, FieldSerializer.class);
            
            // configure compatibility serializer
            kryo.addDefaultSerializer(FeatureId.class, new VersionedSerializer<FeatureId>(
                ImmutableMap.<Integer, Serializer<FeatureId>>builder()
                    .put(1, new FeatureIdSerializerLongIds(kryo, idScope))
                    //.put(2, new FeatureIdSerializer(kryo, idScope))
                    .build(), 1));
        };
    }
}