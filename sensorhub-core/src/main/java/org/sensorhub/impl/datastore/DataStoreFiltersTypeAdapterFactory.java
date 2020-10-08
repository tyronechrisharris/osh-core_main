/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Set;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.api.datastore.RangeFilter;
import org.sensorhub.api.datastore.SpatialFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.TextFilter;
import org.sensorhub.api.datastore.SpatialFilter.SpatialOp;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;


public class DataStoreFiltersTypeAdapterFactory implements TypeAdapterFactory
{       
    
    @SuppressWarnings("unchecked")
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type)
    {
          Class<T> rawType = (Class<T>) type.getRawType();
          
          if (rawType == Range.class)
              return (TypeAdapter<T>)new RangeTypeAdapter().nullSafe();
          else if (rawType == Geometry.class)
              return (TypeAdapter<T>)new GeometryTypeAdapter().nullSafe();
          else if (rawType == TemporalFilter.class)
              return (TypeAdapter<T>)new TemporalFilterTypeAdapter(gson).nullSafe();
          else if (rawType == SpatialFilter.class)
              return (TypeAdapter<T>)new SpatialFilterTypeAdapter(gson).nullSafe();
          else if (IQueryFilter.class.isAssignableFrom(rawType))
              return (TypeAdapter<T>)new QueryFilterTypeAdapter(gson, this).nullSafe();
          
          return null;
    }
    
    
    public static class GeometryTypeAdapter extends TypeAdapter<Geometry>
    {
        public void write(JsonWriter writer, Geometry geom) throws IOException
        {
            writer.beginObject();
            writer.name("type").value(geom.getGeometryType());
            writer.name("coordinates");
            if (geom.getNumPoints() > 1)
                writer.beginArray();
            for (Coordinate coord: geom.getCoordinates())
            {
                writer.beginArray();
                writer.value(coord.x);
                writer.value(coord.y);
                if (!Double.isNaN(coord.z))
                    writer.value(coord.z);
                writer.endArray();
            }
            if (geom.getNumPoints() > 1)
                writer.endArray();
            writer.endObject();
        }

        public Geometry read(JsonReader reader) throws IOException
        {
            reader.beginObject();
            
            while (reader.hasNext())
            {
                String name = reader.nextName();
                
                // ignore any other property
                reader.skipValue();
            }
            
            reader.endObject();
            
            return null;
        }
    }
    
    
    public static class SpatialFilterTypeAdapter extends TypeAdapter<SpatialFilter>
    {
        private final TypeAdapter<Geometry> geomType;
        
        SpatialFilterTypeAdapter(Gson gson)
        {
            this.geomType = gson.getAdapter(Geometry.class);
        }
        
        public void write(JsonWriter writer, SpatialFilter filter) throws IOException
        {
            writer.beginObject();
            
            writer.name("operator").value(filter.getOperator().toString());
            
            if (filter.getOperator() == SpatialOp.DISTANCE)
            {
                writer.name("center");
                geomType.write(writer, filter.getCenter());
                writer.name("distance").value(filter.getDistance());
            }
            else
            {
                writer.name("roi");
                geomType.write(writer, filter.getRoi());
            }
            
            writer.endObject();
        }

        public SpatialFilter read(JsonReader reader) throws IOException
        {
            reader.beginObject();
            
            Point center = null;
            double distance = Double.NaN;
            var builder = new SpatialFilter.Builder();
            
            while (reader.hasNext())
            {
                String name = reader.nextName();
                if ("type".equals(name))
                    builder.withOperator(SpatialOp.valueOf(reader.nextString()));
                else if ("roi".equals(name))
                    builder.withRoi(geomType.read(reader));
                else if ("distance".equals(name))
                    distance = reader.nextDouble();
                else if ("center".equals(name))
                    center = (Point)geomType.read(reader);
                
                // ignore any other property
                reader.skipValue();
            }
                        
            reader.endObject();
            
            if (center != null)
                builder.withDistanceToPoint(center, distance);
            
            return builder.build();
        }
    }


    public class TemporalFilterTypeAdapter extends TypeAdapter<TemporalFilter>
    {
        private final Gson gson;
        
        TemporalFilterTypeAdapter(Gson gson)
        {
            this.gson = gson;
        }
        
        public void write(JsonWriter writer, TemporalFilter filter) throws IOException
        {
            writer.beginObject();
            if (filter.isLatestTime())
                writer.name("indeterminate").value("latest");
            else if (filter.isCurrentTime())
            {
                writer.name("indeterminate").value("current");
                writer.name("tolerance").value(0);
            }
            else
            {
                writer.name("during");
                gson.getAdapter(Range.class).write(writer, filter.getRange());
            }
            writer.endObject();
        }
    
        public TemporalFilter read(JsonReader reader) throws IOException
        {
            reader.beginObject();
            reader.endObject();
            return null;
        }
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static class RangeFilterTypeAdapter extends TypeAdapter<RangeFilter>
    {
        private final Gson gson;
        
        RangeFilterTypeAdapter(Gson gson)
        {
            this.gson = gson;
        }
        
        public void write(JsonWriter writer, RangeFilter filter) throws IOException
        {
            gson.getAdapter(Range.class).write(writer, filter.getRange());
        }

        public RangeFilter read(JsonReader reader) throws IOException
        {
            var range = gson.getAdapter(Range.class).read(reader);
            return new RangeFilter.Builder().withRange(range).build();
        }
    }

    
    public static class RangeTypeAdapter extends TypeAdapter<Range<?>>
    {
        public void write(JsonWriter writer, Range<?> range) throws IOException
        {
            if (range.lowerEndpoint() instanceof Integer)
            {
                if ((int)range.lowerEndpoint() == Integer.MAX_VALUE &&
                    (int)range.upperEndpoint() == Integer.MAX_VALUE)
                {
                    writer.beginObject().name("indeterminate").value("latest").endObject();
                    return;
                }
            }
            
            writer.beginArray();
            writer.value(range.lowerEndpoint().toString());
            writer.value(range.upperEndpoint().toString());
            writer.endArray();
        }

        public Range<?> read(JsonReader reader) throws IOException
        {
            return null;
        }
    }
    
    
    public static class TextFilterTypeAdapter extends TypeAdapter<TextFilter>
    {
        public void write(JsonWriter writer, TextFilter filter) throws IOException
        {
            writer.beginArray();
            for (var s: filter.getKeywords())
                writer.value(s);
            writer.endArray();
        }

        public TextFilter read(JsonReader reader) throws IOException
        {
            return null;
        }
    }
    
    
    public static class QueryFilterTypeAdapter extends TypeAdapter<IQueryFilter>
    {
        static String LIMIT_FIELD = "limit";
        
        final Gson gson;
        final TypeAdapter<JsonElement> jsonEltAdapter;
        final DataStoreFiltersTypeAdapterFactory factory;
                     
        QueryFilterTypeAdapter(Gson gson, DataStoreFiltersTypeAdapterFactory factory)
        {
            this.gson = gson;
            this.jsonEltAdapter = gson.getAdapter(JsonElement.class);
            this.factory = factory;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public void write(JsonWriter writer, IQueryFilter filter) throws IOException
        {
            var delegate = (TypeAdapter)gson.getDelegateAdapter(factory, TypeToken.get(filter.getClass()));
            
            JsonObject jsObj = (JsonObject)delegate.toJsonTree(filter);
            if (filter.getLimit() == Long.MAX_VALUE)
                jsObj.remove(LIMIT_FIELD);
            
            jsonEltAdapter.write(new FormattingJsonWriter(writer), jsObj);
        }

        public IQueryFilter read(JsonReader reader) throws IOException
        {
            return null;
        }
    }
    
    
    public static class FormattingJsonWriter extends JsonWriter
    {
        static Set<String> inlineArrayFields = Sets.newHashSet(
            "coordinates", "internalIDs", "parentIDs");
        
        JsonWriter delegate;
        String lastName;
        boolean nestedArray;
        
        public FormattingJsonWriter(JsonWriter delegate)
        {
            super(new StringWriter());
            this.delegate = delegate;
        }

        public boolean isLenient()
        {
            return delegate.isLenient();
        }

        public JsonWriter beginArray() throws IOException
        {
            if (nestedArray)
                delegate.setIndent("  ");
            delegate.beginArray();
            nestedArray = true;
            if (inlineArrayFields.contains(lastName))
                delegate.setIndent("");
            return this;
        }

        public JsonWriter endArray() throws IOException
        {
            delegate.endArray();
            delegate.setIndent("  ");
            return this;
        }

        public JsonWriter beginObject() throws IOException
        {
            delegate.beginObject();
            return this;
        }

        public JsonWriter endObject() throws IOException
        {
            delegate.endObject();
            return this;
        }

        public JsonWriter name(String name) throws IOException
        {
            this.lastName = name;
            delegate.name(name);
            return this;
        }

        public JsonWriter value(String value) throws IOException
        {
            delegate.value(value);
            return this;
        }

        public JsonWriter jsonValue(String value) throws IOException
        {
            delegate.jsonValue(value);
            return this;
        }

        public JsonWriter nullValue() throws IOException
        {
            delegate.nullValue();
            return this;
        }

        public JsonWriter value(boolean value) throws IOException
        {
            delegate.value(value);
            return this;
        }

        public JsonWriter value(Boolean value) throws IOException
        {
            delegate.value(value);
            return this;
        }

        public JsonWriter value(double value) throws IOException
        {
            delegate.value(value);
            return this;
        }

        public JsonWriter value(long value) throws IOException
        {
            delegate.value(value);
            return this;
        }

        public JsonWriter value(Number value) throws IOException
        {
            delegate.value(value);
            return this;
        }

        public void flush() throws IOException
        {
            delegate.flush();
        }

        public void close() throws IOException
        {
            delegate.close();
        }
    }
    
    
    public static class FieldNamingStrategy implements com.google.gson.FieldNamingStrategy
    {
        static HashMap<String, String> serializedNames = new HashMap<>();
        
        public FieldNamingStrategy()
        {
            serializedNames.put("procFilter", "withProcedures");
            serializedNames.put("obsFilter", "withObservations");
            serializedNames.put("dataStreamFilter", "withDatastreams");
            serializedNames.put("foiFilter", "withFois");
            serializedNames.put("parentFilter", "withParents");
            serializedNames.put("memberFilter", "withMembers");
            serializedNames.put("valuePredicate", "predicate");
        }
        
        @Override
        public String translateName(Field f)
        {
            String jsonName = serializedNames.get(f.getName());
            if (jsonName != null && IQueryFilter.class.isAssignableFrom(f.getDeclaringClass()))
                return jsonName;
            else
                return f.getName();
        }
        
    }
}