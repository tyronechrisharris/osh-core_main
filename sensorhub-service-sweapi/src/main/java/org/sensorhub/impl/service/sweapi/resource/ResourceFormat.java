/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.vast.ogc.gml.GeoJsonBindings;


public class ResourceFormat
{
    static Map<String, ResourceFormat> byMimeType = new HashMap<>();
    
    public static ResourceFormat JSON = new ResourceFormat("application/json");
    public static ResourceFormat GEOJSON = new ResourceFormat(GeoJsonBindings.MIME_TYPE);
    
    public static ResourceFormat SML_JSON = new ResourceFormat("application/sml+json");
    public static ResourceFormat SML_XML = new ResourceFormat("application/sml+xml");
    
    public static ResourceFormat OM_JSON = new ResourceFormat("application/om+json");
    public static ResourceFormat OM_XML = new ResourceFormat("application/om+xml");
    
    public static String SWE_FORMAT_PREFIX = "application/swe+";
    public static ResourceFormat SWE_JSON = new ResourceFormat(SWE_FORMAT_PREFIX + "json");
    public static ResourceFormat SWE_XML = new ResourceFormat(SWE_FORMAT_PREFIX + "xml");
    public static ResourceFormat SWE_BINARY = new ResourceFormat(SWE_FORMAT_PREFIX + "binary");
    public static ResourceFormat SWE_TEXT = new ResourceFormat(SWE_FORMAT_PREFIX + "csv");
    
    public static ResourceFormat TEXT_PLAIN = new ResourceFormat("text/plain");
    public static ResourceFormat TEXT_CSV = new ResourceFormat("text/csv");
    public static ResourceFormat TEXT_XML = new ResourceFormat("text/xml");
    public static ResourceFormat APPLI_XML = new ResourceFormat("application/xml");
    
    
    static
    {
        byMimeType.put("application/json", JSON);
        byMimeType.put(GeoJsonBindings.MIME_TYPE, GEOJSON);
    }
    
    
    String mimeType;
    
    
    public static ResourceFormat fromMimeType(String mimeType)
    {
        if (mimeType == null)
            return null;
        
        var f = byMimeType.get(mimeType);
        if (f == null)
            f = new ResourceFormat(mimeType);
        
        return f;
    }
    
    
    private ResourceFormat(String mimeType)
    {
        this.mimeType = mimeType;
        byMimeType.put(mimeType, this);
    }
    
    
    public String getMimeType()
    {
        return mimeType;
    }
    
    
    public boolean equals(ResourceFormat format)
    {
        return Objects.equals(format.mimeType, mimeType);
    }
    
    
    public boolean isOneOf(ResourceFormat... formats)
    {
        for (var f: formats)
        {
            if (f.equals(this))
                return true;
        }
        
        return false;
    }
    
    
    public String toString()
    {
        return mimeType;
    }
    
}
