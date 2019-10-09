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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.BaseBuilder;
import com.google.common.collect.Range;
import net.opengis.swe.v20.DataComponent;


/**
 * <p>
 * Immutable filter object for procedure data streams.<br/>
 * There is an implicit AND between all filter parameters.<br/>
 * If internal IDs are used, no other filter options are allowed.
 * </p>
 *
 * @author Alex Robin
 * @date Sep 17, 2019
 */
public class DataStreamFilter implements IQueryFilter, Predicate<DataStreamInfo>
{
    public static final Range<Integer> LAST_VERSION = Range.singleton(Integer.MAX_VALUE);
    
    protected SortedSet<Long> internalIDs;
    protected ProcedureFilter procedures;
    protected Set<String> outputNames;
    protected Range<Integer> versions = LAST_VERSION;
    protected Range<Instant> resultTimes;
    protected Set<String> observedProperties;
    protected Predicate<DataStreamInfo> valuePredicate;
    protected long limit = Long.MAX_VALUE;
    

    public SortedSet<Long> getInternalIDs()
    {
        return internalIDs;
    }


    public ProcedureFilter getProcedureFilter()
    {
        return procedures;
    }


    public Set<String> getOutputNames()
    {
        return outputNames;
    }


    public Range<Integer> getVersions()
    {
        return versions;
    }


    public Range<Instant> getResultTimes()
    {
        return resultTimes;
    }


    public Set<String> getObservedProperties()
    {
        return observedProperties;
    }


    public Predicate<DataStreamInfo> getValuePredicate()
    {
        return valuePredicate;
    }
    

    @Override
    public long getLimit()
    {
        return limit;
    }
    
    
    public boolean testOutputName(DataStreamInfo ds)
    {
        return (outputNames == null ||
            outputNames.contains(ds.getOutputName()));
    }
    
    
    public boolean testVersion(DataStreamInfo ds)
    {
        return (versions == null ||
            versions.contains(ds.getRecordVersion()));
    }
    
    
    public boolean testObservedProperty(DataStreamInfo ds)
    {
        if (observedProperties == null)
            return true;
        
        return hasObservable(ds.getRecordDescription());
    }
    
    
    public boolean hasObservable(DataComponent comp)
    {
        String def = comp.getDefinition();
        if (def != null && observedProperties.contains(def))
            return true;
        
        for (int i = 0; i < comp.getComponentCount(); i++)
        {
            if (hasObservable(comp.getComponent(i)))
                return true;
        }
        
        return false;
    }
    
    
    public boolean testValuePredicate(DataStreamInfo v)
    {
        return (valuePredicate == null ||
                valuePredicate.test(v));
    }


    @Override
    public boolean test(DataStreamInfo ds)
    {
        return (testOutputName(ds) &&
            testVersion(ds) &&
            testObservedProperty(ds) &&
            testValuePredicate(ds));
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true, true);
    }
    
    
    public static Builder builder()
    {
        return new Builder();
    }


    public static class Builder extends BaseBuilder<DataStreamFilter>
    {
        protected Builder()
        {
            super(new DataStreamFilter());
        }
        
        
        public Builder from(DataStreamFilter other)
        {
            instance.internalIDs = other.internalIDs;
            instance.procedures = other.procedures;
            instance.outputNames = other.outputNames;
            instance.versions = other.versions;
            instance.resultTimes = other.resultTimes;
            instance.observedProperties = other.observedProperties;
            instance.valuePredicate = other.valuePredicate;
            instance.limit = other.limit;
            return this;
        }
        
        
        public Builder withInternalIDs(Long... ids)
        {
            return withInternalIDs(Arrays.asList(ids));
        }
        
        
        public Builder withInternalIDs(Collection<Long> ids)
        {
            instance.internalIDs = new TreeSet<Long>();
            instance.internalIDs.addAll(ids);
            return this;
        }


        public Builder withProcedures(ProcedureFilter procFilter)
        {
            instance.procedures = procFilter;
            return this;
        }


        public Builder withProcedures(Long... procIDs)
        {
            instance.procedures = ProcedureFilter.builder()
                .withInternalIDs(procIDs)
                .build();
            return this;
        }


        public Builder withProcedures(Collection<Long> procIDs)
        {
            instance.procedures = ProcedureFilter.builder()
                .withInternalIDs(procIDs)
                .build();
            return this;
        }


        public Builder withProcedures(String... procUIDs)
        {
            instance.procedures = ProcedureFilter.builder()
                .withUniqueIDs(procUIDs)
                .build();
            return this;
        }
        
        
        public Builder withOutputNames(String... names)
        {
            instance.outputNames = new HashSet<String>();
            for (String name: names)
                instance.outputNames.add(name);
            return this;
        }


        public Builder withVersion(int version)
        {
            instance.versions = Range.singleton(version);
            return this;
        }


        public Builder withVersionRange(int minVersion, int maxVersion)
        {
            instance.versions = Range.closed(minVersion, maxVersion);
            return this;
        }


        public Builder withResultTimeRange(Instant begin, Instant end)
        {
            instance.resultTimes = Range.closed(begin, end);
            return this;
        }
        
        
        public Builder withObservedProperties(String... uris)
        {
            instance.observedProperties = new TreeSet<String>();
            for (String uri: uris)
                instance.observedProperties.add(uri);
            return this;
        }


        public Builder withValuePredicate(Predicate<DataStreamInfo> valuePredicate)
        {
            instance.valuePredicate = valuePredicate;
            return this;
        }
        
        
        public DataStreamFilter build()
        {
            // make all collections immutable
            if (instance.internalIDs != null)
                instance.internalIDs = Collections.unmodifiableSortedSet(instance.internalIDs);
            if (instance.outputNames != null)
                instance.outputNames = Collections.unmodifiableSet(instance.outputNames);
            if (instance.observedProperties != null)
                instance.observedProperties = Collections.unmodifiableSet(instance.observedProperties);
            
            return super.build();
        }
    }

}
