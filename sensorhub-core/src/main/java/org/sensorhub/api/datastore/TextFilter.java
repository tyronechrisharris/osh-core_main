/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
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
