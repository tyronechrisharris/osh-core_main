/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.resource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.sensorhub.api.datastore.IQueryFilter;
import org.sensorhub.api.datastore.TextFilter;
import org.vast.util.BaseBuilder;
import org.vast.util.IResource;


/**
 * <p>
 * Immutable filter for any resource type.<br/>
 * It serves as a base for more advanced resource-specific filters.<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @param <T> Type of resource supported by this filter
 * @date Oct 8, 2018
 */
public class ResourceFilter<T extends IResource> implements IQueryFilter, Predicate<T>
{
    protected Set<Long> internalIDs;
    protected Set<Long> parentIDs;
    protected TextFilter fullText;
    protected Predicate<T> valuePredicate;
    protected long limit = Long.MAX_VALUE;
    
    
    protected ResourceFilter() {}
    
    
    public Set<Long> getInternalIDs()
    {
        return internalIDs;
    }


    public Set<Long> getParentIDs()
    {
        return parentIDs;
    }


    public TextFilter getFullText()
    {
        return fullText;
    }


    public Predicate<T> getValuePredicate()
    {
        return valuePredicate;
    }


    @Override
    public long getLimit()
    {
        return limit;
    }
    
    
    public boolean testValuePredicate(T res)
    {
        return (valuePredicate == null ||
                valuePredicate.test(res));
    }


    @Override
    public boolean test(T res)
    {
        return testValuePredicate(res);
    }
    
    
    public void validate()
    {
    }


    public static class Builder extends ResourceFilterBuilder<Builder, IResource, ResourceFilter<IResource>>
    {
        public Builder()
        {
            super(new ResourceFilter<>());
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static class ResourceFilterBuilder<
            B extends ResourceFilterBuilder<B, T, F>,
            T extends IResource,
            F extends ResourceFilter<T>>
        extends BaseBuilder<F>
    {
        
        protected ResourceFilterBuilder(F instance)
        {
            super(instance);
        }
        
        
        /**
         * Keep only resources with specific internal IDs.
         * @param ids One or more internal IDs of resources to select
         * @return This builder for chaining
         */
        public B withInternalIDs(Long... ids)
        {
            return withInternalIDs(Arrays.asList(ids));
        }
        
        
        /**
         * Keep only resources with specific internal IDs.
         * @param ids Collection of internal IDs
         * @return This builder for chaining
         */
        public B withInternalIDs(Collection<Long> ids)
        {
            if (instance.internalIDs == null)
                instance.internalIDs = new TreeSet<>();
            instance.internalIDs.addAll(ids);
            return (B)this;
        }
        
        
        public B withParents(Long... parentIDs)
        {
            if (instance.parentIDs == null)
                instance.parentIDs = new TreeSet<>();
            instance.parentIDs.addAll(Arrays.asList(parentIDs));
            return (B)this;
        }


        public TextFilter.NestedBuilder<B> withFullText()
        {
            return new TextFilter.NestedBuilder<B>((B)this) {
                @Override
                public B done()
                {
                    ResourceFilterBuilder.this.instance.fullText = build();
                    return (B)ResourceFilterBuilder.this;
                }                
            };
        }
        
        
        public B withKeywords(String... keywords)
        {
            instance.fullText = new TextFilter.Builder()
                .withKeywords(keywords)
                .build();
            return (B)this;
        }


        public B withValuePredicate(Predicate<T> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return (B)this;
        }
        
        
        public B withLimit(int limit)
        {
            instance.limit = limit;
            return (B)this;
        }
        
        
        @Override
        public F build()
        {
            instance.validate();
            return super.build();
        }
    }
}
