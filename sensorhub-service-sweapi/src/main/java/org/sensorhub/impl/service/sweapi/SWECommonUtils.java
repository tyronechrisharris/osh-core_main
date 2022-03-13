/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import java.util.Set;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.vast.data.JSONEncodingImpl;
import org.vast.data.TextEncodingImpl;
import org.vast.data.XMLEncodingImpl;
import org.vast.swe.IComponentFilter;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import com.google.common.collect.ImmutableSet;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import net.opengis.swe.v20.TextEncoding;


public class SWECommonUtils
{
    
    public static final Set<String> OM_COMPONENTS_DEF = ImmutableSet.of(
        SWEConstants.DEF_PHENOMENON_TIME,
        SWEConstants.DEF_SAMPLING_TIME,
        SWEConstants.DEF_FORECAST_TIME,
        SWEConstants.DEF_RUN_TIME
    );
    
    /*
     * Filter to skip properties already provided by O&M from result
     * e.g. time stamp and feature of interest ID
     */
    public static final IComponentFilter OM_COMPONENTS_FILTER = new IComponentFilter() {
        @Override
        public boolean accept(DataComponent comp)
        {
            var def = comp.getDefinition();
            if (comp.getParent() == null || OM_COMPONENTS_DEF.contains(def))
                return false;
            else
                return true;
        }
    };
    
    
    /*
     * Choose proper encoding for the selected SWE Common sub format
     * @throws InvalidRequestException if sub format is not supported
     */
    public static DataEncoding getEncoding(IDataStreamInfo dsInfo, ResourceFormat format) throws InvalidRequestException
    {
        // init SWE datastream writer depending on desired format and native encoding
        if (dsInfo.getRecordEncoding() instanceof TextEncoding)
        {
            if (format.equals(ResourceFormat.SWE_JSON))
            {
                return new JSONEncodingImpl();
            }
            else if (format.equals(ResourceFormat.SWE_TEXT))
            {
                return dsInfo.getRecordEncoding();
            } 
            else if (format.equals(ResourceFormat.SWE_XML))
            {
                return new XMLEncodingImpl();
            }
            else if (format.equals(ResourceFormat.SWE_BINARY))
            {
                return SWEHelper.getDefaultBinaryEncoding(dsInfo.getRecordStructure());
            }
        }
        else if (dsInfo.getRecordEncoding() instanceof BinaryEncoding)
        {
            if (format.equals(ResourceFormat.SWE_BINARY))
            {
                return dsInfo.getRecordEncoding();
            }
            else if (ResourceFormat.allowNonBinaryFormat(dsInfo.getRecordEncoding()))
            {
                if (format.equals(ResourceFormat.SWE_JSON))
                {
                    return new JSONEncodingImpl();
                }
                else if (format.isOneOf(ResourceFormat.SWE_TEXT, ResourceFormat.TEXT_PLAIN, ResourceFormat.TEXT_CSV))
                {
                    return new TextEncodingImpl();
                }
                else if (format.isOneOf(ResourceFormat.SWE_XML, ResourceFormat.TEXT_XML))
                {
                    return new XMLEncodingImpl();
                }
            }
        }
        
        throw ServiceErrors.unsupportedFormat(format);
    }
}
