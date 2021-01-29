/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.kryo.v1;

import javax.xml.namespace.QName;
import org.vast.ogc.gml.GMLStaxBindings;
import org.vast.ogc.gml.GenericFeature;
import org.vast.ogc.gml.GenericTemporalFeatureImpl;
import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.TimeExtent;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import net.opengis.gml.v32.AbstractGeometry;
import net.opengis.gml.v32.LineString;
import net.opengis.gml.v32.LinearRing;
import net.opengis.gml.v32.Point;
import net.opengis.gml.v32.Polygon;
import net.opengis.gml.v32.impl.GMLFactory;


/**
 * <p>
 * Kryo serializer/deserializer for Feature objects.<br/>
 * </p>
 *
 * @author Alex Robin
 * @date Dec 2, 2020
 */
public class FeatureSerializer extends Serializer<IFeature>
{
    static final int VERSION = 1;
    static final String UNKNOWN_GEOM_ERROR = "Unsupported geometry type: ";
    static final QName DEFAULT_QNAME = new QName(GMLStaxBindings.NS_URI, "Feature");
    static enum GeomType {NULL, POINT, LINESTRING, POLYGON}
    
    GMLFactory gmlFactory = new GMLFactory(true);
    
    
    public FeatureSerializer()
    {
    }
    
    
    @Override
    public void write(Kryo kryo, Output output, IFeature f)
    {
        //output.writeString(f.getId());
        output.writeString(f instanceof GenericFeature ? ((GenericFeature)f).getQName().toString() : "Feature");
        
        // common properties
        output.writeString(f.getUniqueIdentifier());
        output.writeString(f.getName());
        output.writeString(f.getDescription());
        
        // geometry
        AbstractGeometry geom = null;
        if (f instanceof IGeoFeature)
            geom = ((IGeoFeature) f).getGeometry();
        writeGeometry(output, geom);
        
        // valid time
        TimeExtent validTime = null;
        if (f instanceof ITemporalFeature)
            validTime = ((ITemporalFeature) f).getValidTime();
        KryoUtils.writeTimeExtent(output, validTime);
        
        // properties
        output.writeVarInt(f.getProperties().size(), true);
        for (var prop: f.getProperties().entrySet())
        {
            output.writeString(prop.getKey().toString());
            kryo.writeClassAndObject(output, prop.getValue());
        }
    }
    
    
    @Override
    public IFeature read(Kryo kryo, Input input, Class<IFeature> type)
    {
        String fType = input.readString();
        QName qname = fType != null ? QName.valueOf(fType) : DEFAULT_QNAME;
        var f = new GenericTemporalFeatureImpl(qname);
        
        // common properties
        f.setUniqueIdentifier(input.readString());
        f.setName(input.readString());
        f.setDescription(input.readString());
        
        // geom
        var geom = readGeometry(input);
        if (geom != null)
            f.setGeometry(geom);
        
        // valid time
        var validTime = KryoUtils.readTimeExtent(input);
        if (validTime != null)
            f.setValidTime(validTime);
        
        // properties
        int numProperties = input.readVarInt(true);
        for (int i = 0; i < numProperties; i++)
        {
            var qName = QName.valueOf(input.readString());
            var value = kryo.readClassAndObject(input);
            f.getProperties().put(qName, value);
        }
        
        return f;
    }
    
    
    protected void writeGeometry(Output output, AbstractGeometry g)
    {
        // geom type
        if (g == null)
        {
            output.writeByte(GeomType.NULL.ordinal());
            return;
        }
        
        // SRS name and dimensionality
        output.writeString(g.getSrsName());
        output.writeByte(g.getSrsDimension());        
        
        if (g instanceof Point)
        {
            output.writeByte(GeomType.POINT.ordinal());
            writePosList(output, ((Point) g).getPos());
        }
        else if (g instanceof LineString)
        {
            output.writeByte(GeomType.LINESTRING.ordinal());
            writePosList(output, ((LineString) g).getPosList());
        }
        else if (g instanceof Polygon)
        {
            output.writeByte(GeomType.POLYGON.ordinal());
            // exterior
            double[] posList = ((Polygon) g).getExterior().getPosList();
            writePosList(output, posList);
            // holes
            int numHoles = ((Polygon) g).getNumInteriors();
            output.writeVarInt(numHoles, true);
            if (numHoles > 0)
            {
                for (LinearRing hole: ((Polygon) g).getInteriorList())
                    writePosList(output, hole.getPosList());
            }
        }
        else
            throw new IllegalStateException(UNKNOWN_GEOM_ERROR + g.getClass().getCanonicalName());
    }
    
    
    protected void writePosList(Output output, double[] posList)
    {
        output.writeVarInt(posList.length, true);
        output.writeDoubles(posList);
    }
    
    
    protected AbstractGeometry readGeometry(Input input)
    {
        int geomTypeIndex = input.readByte();
        if (geomTypeIndex == GeomType.NULL.ordinal())
            return null;
        if (geomTypeIndex > GeomType.values().length)
            throw new IllegalStateException(UNKNOWN_GEOM_ERROR + geomTypeIndex);
                
        // SRS name and dimensionality
        var srsName = input.readString();
        var srsDims = input.readByte();
        
        var geomType = GeomType.values()[geomTypeIndex];
        AbstractGeometry g = null;
        switch (geomType)
        {
            case POINT:
                g = gmlFactory.newPoint();
                ((Point)g).setPos(readPosList(input));
                break;
                
            case LINESTRING:
                g = gmlFactory.newLineString();
                ((LineString)g).setPosList(readPosList(input));
                break;
                
            case POLYGON:
                g = gmlFactory.newPolygon();
                ((Polygon)g).setExterior(readLinearRing(input));
                int numHoles = input.readVarInt(true);
                for (int i=0; i<numHoles;i++)
                    ((Polygon)g).addInterior(readLinearRing(input));
                break;
                
            default:
                throw new IllegalStateException(UNKNOWN_GEOM_ERROR + geomType);
        }
        
        if (srsName != null)
            g.setSrsName(srsName);
        g.setSrsDimension(srsDims);
        
        return g;
    }
    
    
    protected double[] readPosList(Input input)
    {
        int l = input.readVarInt(true);
        return input.readDoubles(l);
    }
    
    
    protected LinearRing readLinearRing(Input input)
    {
        LinearRing ring = gmlFactory.newLinearRing();
        ring.setPosList(readPosList(input));
        return ring;
    }
}
