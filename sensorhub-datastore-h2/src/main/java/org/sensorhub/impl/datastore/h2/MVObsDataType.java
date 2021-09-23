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
import java.util.HashMap;
import java.util.Map.Entry;
import org.sensorhub.api.obs.ObsData;
import org.vast.data.AbstractDataBlock;
import org.vast.data.DataBlockBoolean;
import org.vast.data.DataBlockByte;
import org.vast.data.DataBlockCompressed;
import org.vast.data.DataBlockDouble;
import org.vast.data.DataBlockFloat;
import org.vast.data.DataBlockInt;
import org.vast.data.DataBlockLong;
import org.vast.data.DataBlockMixed;
import org.vast.data.DataBlockParallel;
import org.vast.data.DataBlockShort;
import org.vast.data.DataBlockString;
import org.vast.data.DataBlockTuple;
import org.vast.data.DataBlockUByte;
import org.vast.data.DataBlockUInt;
import org.vast.data.DataBlockUShort;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import net.opengis.gml.v32.impl.JTSCoordinatesDoubleArray;


/**
 * <p>
 * H2 DataType implementation for ObsData objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVObsDataType extends KryoDataType
{
    MVObsDataType()
    {
        this.configurator = kryo -> {
            
            // pre-register known types with Kryo
            kryo.register(ObsData.class, 20);
            kryo.register(Instant.class, 30);
            kryo.register(HashMap.class, 31);
            kryo.register(Entry.class, 32);
            kryo.register(Entry[].class, 33);
            kryo.register(DataBlockBoolean.class, 100);
            kryo.register(DataBlockByte.class, 101);
            kryo.register(DataBlockUByte.class, 102);
            kryo.register(DataBlockShort.class, 103);
            kryo.register(DataBlockUShort.class, 104);
            kryo.register(DataBlockInt.class, 105);
            kryo.register(DataBlockUInt.class, 106);
            kryo.register(DataBlockLong.class, 107);
            kryo.register(DataBlockFloat.class, 108);
            kryo.register(DataBlockDouble.class, 109);
            kryo.register(DataBlockString.class, 110);
            kryo.register(AbstractDataBlock[].class, 111);
            kryo.register(DataBlockTuple.class, 112);
            kryo.register(DataBlockParallel.class, 113);
            kryo.register(DataBlockMixed.class, 114);
            kryo.register(DataBlockCompressed.class, 115);
            kryo.register(Coordinate.class, 200);
            kryo.register(Coordinate[].class, 201);
            kryo.register(CoordinateArraySequence.class, 202);
            kryo.register(JTSCoordinatesDoubleArray.class, 203);
            kryo.register(Point.class, 210);
            kryo.register(LineString.class, 211);
            kryo.register(LinearRing.class, 212);
            kryo.register(Polygon.class, 213);
        };
    }
}