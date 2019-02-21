/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.

 Contributor(s): 
    Alexandre Robin "alex.robin@sensiasoft.com"
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class TextFilter
{
    private Set<String> keywords = new HashSet<>();
    
    
    public Set<String> getKeywords()
    {
        return keywords;
    }
    
    
    public static class Builder
    {
        protected TextFilter instance;
    
    
        public Builder()
        {
            this.instance = new TextFilter();
        }
        
        
        public Builder withKeywords(String... keywords)
        {
            if (instance.keywords == null)
                instance.keywords = new HashSet<>();
            instance.keywords.addAll(Arrays.asList(keywords));
            return this;
        }
    
    
        public TextFilter build()
        {
            TextFilter newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
