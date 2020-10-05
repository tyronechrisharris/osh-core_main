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
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import org.sensorhub.utils.FilterUtils;
import org.vast.util.BaseBuilder;


/**
 * <p>
 * TODO TextFilter type description
 * </p>
 *
 * @author Alex Robin
 * @date Sep 26, 2020
 */
public class TextFilter
{
    protected Set<String> keywords;
    
    
    public Set<String> getKeywords()
    {
        return keywords;
    }
    
    
    /**
     * Computes a logical AND between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    public TextFilter and(TextFilter filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        return and(filter, new Builder()).build();
    }
    
    
    protected <F extends TextFilter, B extends TextFilterBuilder<B, F>> B and(F otherFilter, B builder) throws EmptyFilterIntersection
    {
        return builder
            .withKeywords(FilterUtils.intersect(this.keywords, otherFilter.keywords));
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends TextFilterBuilder<Builder, TextFilter>
    {
        public Builder()
        {
            super(new TextFilter());
        }
    }
    
    
    /*
     * Nested builder for use within another builder
     */
    public static abstract class NestedBuilder<B> extends TextFilterBuilder<NestedBuilder<B>, TextFilter>
    {
        B parent;
        
        public NestedBuilder(B parent)
        {
            this.parent = parent;
            this.instance = new TextFilter();
        }
                
        public abstract B done();
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class TextFilterBuilder<
            B extends TextFilterBuilder<B, F>,
            F extends TextFilter> extends BaseBuilder<TextFilter>
    {
        public TextFilterBuilder()
        {
            super(new TextFilter());
        }
        
        
        protected TextFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        public B withKeywords(String... keywords)
        {
            return withKeywords(Arrays.asList(keywords));
        }
        
        
        public B withKeywords(Collection<String> keywords)
        {
            if (instance.keywords == null)
                instance.keywords = new TreeSet<>();
            instance.keywords.addAll(keywords);
            return (B)this;
        }
    }
}
