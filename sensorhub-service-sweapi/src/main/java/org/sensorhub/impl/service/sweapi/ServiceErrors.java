/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import org.sensorhub.impl.service.sweapi.InvalidRequestException.ErrorCode;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;


public class ServiceErrors
{
    public static final String UNSUPPORTED_FORMAT_ERROR_MSG = "Unsupported format: ";
    
    
    private ServiceErrors() {}
    
    
    public static InvalidRequestException unsupportedOperation(String msg)
    {
        return new InvalidRequestException(ErrorCode.UNSUPPORTED_OPERATION, msg);
    }
    
    
    public static InvalidRequestException badRequest(String msg)
    {
        return new InvalidRequestException(ErrorCode.BAD_REQUEST, msg);
    }
    
    
    public static InvalidRequestException unsupportedFormat(ResourceFormat format)
    {
        return new InvalidRequestException(ErrorCode.BAD_REQUEST, UNSUPPORTED_FORMAT_ERROR_MSG + format);
    }
    
    
    public static InvalidRequestException notFound() 
    {
        return new InvalidRequestException(ErrorCode.NOT_FOUND, "Resource not found");
    }
    
    
    public static InvalidRequestException notFound(String id) 
    {
        return new InvalidRequestException(ErrorCode.NOT_FOUND, "Resource not found: " + id);
    }
}
