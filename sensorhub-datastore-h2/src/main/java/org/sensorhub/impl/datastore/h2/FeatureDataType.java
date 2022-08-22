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
import org.locationtech.jts.geom.PrecisionModel;
import org.sensorhub.impl.datastore.h2.kryo.FeatureClassResolver;
import org.sensorhub.impl.datastore.h2.kryo.KryoDataType;
import org.sensorhub.impl.datastore.h2.kryo.v2.PrecisionModelSerializer;
import org.sensorhub.impl.serialization.kryo.VersionedSerializer;
import org.vast.ogc.gml.FeatureRef;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.om.ProcedureRef;
import org.vast.ogc.om.SamplingFeature;
import com.esotericsoftware.kryo.SerializerFactory;
import com.esotericsoftware.kryo.SerializerFactory.SingletonSerializerFactory;
import com.google.common.collect.ImmutableMap;


/**
 * <p>
 * H2 DataType implementation for feature objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class FeatureDataType extends KryoDataType
{
    
    FeatureDataType(MVMap<String, Integer> kryoClassMap)
    {
        this.classResolver = () -> new FeatureClassResolver(kryoClassMap);
        this.configurator = kryo -> {
            
            // setup generic feature serializer
            kryo.addDefaultSerializer(IFeature.class, VersionedSerializer.<IFeature>factory(ImmutableMap.of(
                H2Utils.CURRENT_VERSION, new org.sensorhub.impl.serialization.kryo.v1.FeatureSerializer()),
                H2Utils.CURRENT_VERSION));
            
            // but use default serializer for the following well-known feature types
            kryo.addDefaultSerializer(FeatureRef.class, defaultObjectSerializer);
            kryo.addDefaultSerializer(ProcedureRef.class, defaultObjectSerializer);
            kryo.addDefaultSerializer(SamplingFeature.class, defaultObjectSerializer);
            
            // register serializer backward compatible with JTS < 1.19 classes
            kryo.addDefaultSerializer(PrecisionModel.class, VersionedSerializer.<PrecisionModel>factory2(ImmutableMap.of(
                H2Utils.CURRENT_VERSION, new SerializerFactory.FieldSerializerFactory(),
                1, new SingletonSerializerFactory<>(new PrecisionModelSerializer(kryo))),
                H2Utils.CURRENT_VERSION));
        };
    }
}