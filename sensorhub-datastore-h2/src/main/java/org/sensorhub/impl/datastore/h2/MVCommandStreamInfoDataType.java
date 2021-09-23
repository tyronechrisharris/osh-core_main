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
import java.util.ArrayList;
import java.util.HashMap;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.DataStreamInfo;
import org.vast.data.*;
import net.opengis.OgcPropertyImpl;
import net.opengis.OgcPropertyList;


class MVCommandStreamInfoDataType extends KryoDataType
{
    MVCommandStreamInfoDataType()
    {
        this.configurator = kryo -> {
            
            // pre-register known types with Kryo
            kryo.register(Instant.class, 20);
            kryo.register(ArrayList.class, 21);
            kryo.register(HashMap.class, 22);
            kryo.register(String[].class, 23);
            kryo.register(DataStreamInfo.class, 40);
            kryo.register(FeatureId.class, 41);
            kryo.register(DataRecordImpl.class, 50);
            kryo.register(VectorImpl.class, 51);
            kryo.register(DataArrayImpl.class, 52);
            kryo.register(MatrixImpl.class, 53);
            kryo.register(DataChoiceImpl.class, 54);
            kryo.register(BooleanImpl.class, 55);
            kryo.register(TextImpl.class, 56);
            kryo.register(CategoryImpl.class, 57);
            kryo.register(CountImpl.class, 58);
            kryo.register(QuantityImpl.class, 59);
            kryo.register(TimeImpl.class, 60);
            kryo.register(CategoryRangeImpl.class, 61);
            kryo.register(CountRangeImpl.class, 62);
            kryo.register(QuantityRangeImpl.class, 63);
            kryo.register(TimeRangeImpl.class, 64);
            kryo.register(OgcPropertyImpl.class, 80);
            kryo.register(OgcPropertyList.class, 81);
            kryo.register(DataComponentProperty.class, 82);
            kryo.register(DataComponentPropertyList.class, 83);
            kryo.register(AllowedTokensImpl.class, 84);
            kryo.register(AllowedValuesImpl.class, 85);
            kryo.register(AllowedTimesImpl.class, 86);
            kryo.register(NilValuesImpl.class, 87);
            kryo.register(NilValueImpl.class, 88);
            kryo.register(UnitReferenceImpl.class, 89);
            kryo.register(EncodedValuesImpl.class, 90);
            kryo.register(TextEncodingImpl.class, 91);
            kryo.register(JSONEncodingImpl.class, 92);
            kryo.register(XMLEncodingImpl.class, 93);
            kryo.register(BinaryEncodingImpl.class, 94);
            kryo.register(BinaryComponentImpl.class, 95);
            kryo.register(BinaryBlockImpl.class, 96);
            kryo.register(DateTimeOrDouble.class, 97);
        };
    }
}