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


class MVDataStreamInfoDataType extends KryoDataType
{
    MVDataStreamInfoDataType()
    {
        // pre-register known types with Kryo
        registeredClasses.put(20, Instant.class);
        registeredClasses.put(21, ArrayList.class);
        registeredClasses.put(22, HashMap.class);
        registeredClasses.put(23, String[].class);
        registeredClasses.put(40, DataStreamInfo.class);
        registeredClasses.put(41, FeatureId.class);
        registeredClasses.put(50, DataRecordImpl.class);
        registeredClasses.put(51, VectorImpl.class);
        registeredClasses.put(52, DataArrayImpl.class);
        registeredClasses.put(53, MatrixImpl.class);
        registeredClasses.put(54, DataChoiceImpl.class);
        registeredClasses.put(55, BooleanImpl.class);
        registeredClasses.put(56, TextImpl.class);
        registeredClasses.put(57, CategoryImpl.class);
        registeredClasses.put(58, CountImpl.class);
        registeredClasses.put(59, QuantityImpl.class);
        registeredClasses.put(60, TimeImpl.class);
        registeredClasses.put(61, CategoryRangeImpl.class);
        registeredClasses.put(62, CountRangeImpl.class);
        registeredClasses.put(63, QuantityRangeImpl.class);
        registeredClasses.put(64, TimeRangeImpl.class);
        registeredClasses.put(80, OgcPropertyImpl.class);
        registeredClasses.put(81, OgcPropertyList.class);
        registeredClasses.put(82, DataComponentProperty.class);
        registeredClasses.put(83, DataComponentPropertyList.class);
        registeredClasses.put(84, AllowedTokensImpl.class);
        registeredClasses.put(85, AllowedValuesImpl.class);
        registeredClasses.put(86, AllowedTimesImpl.class);
        registeredClasses.put(87, NilValuesImpl.class);
        registeredClasses.put(88, NilValueImpl.class);
        registeredClasses.put(89, UnitReferenceImpl.class);
        registeredClasses.put(90, EncodedValuesImpl.class);
        registeredClasses.put(91, TextEncodingImpl.class);
        registeredClasses.put(92, JSONEncodingImpl.class);
        registeredClasses.put(93, XMLEncodingImpl.class);
        registeredClasses.put(94, BinaryEncodingImpl.class);
        registeredClasses.put(95, BinaryComponentImpl.class);
        registeredClasses.put(96, BinaryBlockImpl.class);
        registeredClasses.put(97, DateTimeOrDouble.class);
    }
}