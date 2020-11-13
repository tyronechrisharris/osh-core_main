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

import org.vast.util.BaseBuilder;


public class ResourceLink
{
    String type;
    String rel;
    String title;
    String href;
    

    public String getType()
    {
        return type;
    }


    public String getRel()
    {
        return rel;
    }


    public String getTitle()
    {
        return title;
    }


    public String getHref()
    {
        return href;
    }
    
    
    public static class Builder extends BaseBuilder<ResourceLink>
    {
        Builder()
        {
            this.instance = new ResourceLink();
        }
        
        
        public Builder type(String type)
        {
            instance.type = type;
            return this;
        }
        
        
        public Builder rel(String type)
        {
            instance.type = type;
            return this;
        }
        
        
        public Builder title(String title)
        {
            instance.title = title;
            return this;
        }
        
        
        public Builder href(String href)
        {
            instance.href = href;
            return this;
        }
    }
}
