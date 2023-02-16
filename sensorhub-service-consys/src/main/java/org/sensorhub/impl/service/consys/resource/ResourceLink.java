/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.consys.resource;

import org.vast.util.BaseBuilder;


public class ResourceLink
{
    public final static String REL_SELF = "self";
    public final static String REL_PREV = "prev";
    public final static String REL_NEXT = "next";
    public final static String REL_ITEMS = "items";
    public final static String REL_COLLECTION = "collection";
    
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
    
    
    public ResourceLink withFormat(String formatName, String mimeType)
    {
        var newLink = new ResourceLink();
        newLink.rel = this.rel;
        newLink.title = this.title + " as " + formatName;
        newLink.type = mimeType;
        newLink.href = this.href + (this.href.contains("?") ? "&" : "?") + "f=" + mimeType;
        return newLink;
    }
    
    
    public static ResourceLink self(String url, String mimeType)
    {
        return new ResourceLink.Builder()
            .rel(REL_SELF)
            .type(mimeType)
            .title("This document")
            .href(url)
            .build();
    }
    
    
    public static Builder builder()
    {
        return new Builder();
    }
    
    
    public static class Builder extends BaseBuilder<ResourceLink>
    {
        public Builder()
        {
            this.instance = new ResourceLink();
        }
        
        
        public Builder type(String type)
        {
            instance.type = type;
            return this;
        }
        
        
        public Builder rel(String rel)
        {
            instance.rel = rel;
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
