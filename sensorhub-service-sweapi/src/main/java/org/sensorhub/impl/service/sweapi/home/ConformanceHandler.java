/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.home;

import java.io.IOException;
import java.util.Set;
import org.sensorhub.impl.service.sweapi.BaseHandler;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.SWEApiServiceConfig;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import com.google.common.collect.ImmutableSet;


public class ConformanceHandler extends BaseHandler
{
    public static final String[] NAMES = { "conformance" };
    
    static final Set<String> CONF_CLASSES = ImmutableSet.of(
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/html",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
        "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
        "http://www.opengis.net/spec/ogcapi-common-2/0.0/conf/collections",
        "http://www.opengis.net/spec/ogcapi-common-2/0.0/conf/html",
        "http://www.opengis.net/spec/ogcapi-common-2/0.0/conf/json",
        
        "http://www.opengis.net/spec/sweapi-common-1/0.0/conf/core",
        "http://www.opengis.net/spec/sweapi-common-1/0.0/conf/obs",
        "http://www.opengis.net/spec/sweapi-common-1/0.0/conf/html",
        "http://www.opengis.net/spec/sweapi-common-1/0.0/conf/json",
        "http://www.opengis.net/spec/sweapi-common-1/0.0/conf/om+json",
        "http://www.opengis.net/spec/sweapi-common-2/0.0/conf/swe+json",
        "http://www.opengis.net/spec/sweapi-common-2/0.0/conf/swe+csv",
        "http://www.opengis.net/spec/sweapi-common-2/0.0/conf/swe+xml",
        "http://www.opengis.net/spec/sweapi-common-2/0.0/conf/swe+binary",
        "http://www.opengis.net/spec/sweapi-common-3/0.0/conf/tasking",
        "http://www.opengis.net/spec/sweapi-common-3/0.0/conf/subsystems",
        "http://www.opengis.net/spec/sweapi-common-3/0.0/conf/history"
    );
    
    SWEApiServiceConfig serviceConfig;
    
    
    public ConformanceHandler(SWEApiServiceConfig serviceConfig)
    {
        this.serviceConfig = serviceConfig;
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
    
    
    @Override
    public void doGet(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        var format = parseFormat(ctx.getParameterMap());
        
        // set content type
        ctx.setResponseContentType(format.getMimeType());
        
        if (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest())
            new ConformanceHtml(ctx, CONF_CLASSES).serialize(0L, serviceConfig, true);
        else if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON))
            new ConformanceJson(ctx, CONF_CLASSES).serialize(0L, serviceConfig, true);
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        ServiceErrors.unsupportedOperation("");
    }
    

    @Override
    public void doPut(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        ServiceErrors.unsupportedOperation("");
    }
    

    @Override
    public void doDelete(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        ServiceErrors.unsupportedOperation("");
    }

}
