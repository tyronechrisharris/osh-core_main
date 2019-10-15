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

import java.util.SortedSet;
import java.util.TreeSet;


/**
 * <p>
 * Immutable filter object for procedures (e.g. sensors, actuators, procedure groups etc.).<br/>
 * There is an implicit AND between all filter parameters
 * </p>
 *
 * @author Alex Robin
 * @date Apr 2, 2018
 */
public class ProcedureFilter extends FeatureFilter
{
    protected SortedSet<String> parentUIDs;
    protected DataStreamFilter dataStreamFilter;
    protected ObsFilter obsFilter;
    protected FoiFilter foiFilter; // shortcut for ObsFilter/FoiFilter
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected ProcedureFilter() {}
    
    
    public SortedSet<String> getParentGroups()
    {
        return parentUIDs;
    }
    
    
    public DataStreamFilter getDataStreamFilter()
    {
        return dataStreamFilter;
    }
    
    
    public ObsFilter getObservationsFilter()
    {
        return obsFilter;
    }


    public FoiFilter getFoiFilter()
    {
        return foiFilter;
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends ProcedureFilterBuilder<Builder, ProcedureFilter>
    {
        public Builder()
        {
            this.instance = new ProcedureFilter();
        }
        
        protected Builder(ProcedureFilter instance)
        {
            this.instance = instance;
        }
        
        public static Builder from(ProcedureFilter base)
        {
            return new Builder(null).copyFrom(base);
        }
    }
    
    
    @SuppressWarnings("unchecked")
    public static abstract class ProcedureFilterBuilder<
            B extends ProcedureFilterBuilder<B, F>,
            F extends ProcedureFilter>
        extends FeatureFilterBuilder<B, ProcedureFilter>
    {        
        
        protected ProcedureFilterBuilder()
        {
        }
                
        
        protected B copyFrom(ProcedureFilter base)
        {
            super.copyFrom(base);
            instance.parentUIDs = base.parentUIDs;
            instance.dataStreamFilter = base.dataStreamFilter;
            instance.obsFilter = base.obsFilter;
            instance.foiFilter = base.foiFilter;
            return (B)this;
        }
        
        
        /**
         * Select only procedures belonging to the specified groups 
         * @param parentIds IDs of parent groups
         * @return This builder for chaining
         */
        public B withParentGroups(String... parentIds)
        {
            instance.parentUIDs = new TreeSet<String>();            
            for (String id: parentIds)
                instance.parentUIDs.add(id);            
            return (B)this;
        }
        
        
        /**
         * Select only procedures with datastreams matching the filter
         * @param filter Data stream filter
         * @return This builder for chaining
         */
        public B withDataStreams(DataStreamFilter filter)
        {
            instance.dataStreamFilter = filter;
            return (B)this;
        }
        
        
        /**
         * Select only procedures with datastreams matching the filter
         * @param filter Data stream filter
         * @return This builder for chaining
         */
        public B withDataStreams(DataStreamFilter.Builder filter)
        {
            return withDataStreams(filter.build());
        }
        

        /**
         * Select only procedures with observations matching the filter
         * @param filter Observation filter
         * @return This builder for chaining
         */
        public B withObservations(ObsFilter filter)
        {
            instance.obsFilter = filter;
            return (B)this;
        }
        

        /**
         * Select only procedures with observations matching the filter
         * @param filter Observation filter
         * @return This builder for chaining
         */
        public B withObservations(ObsFilter.Builder filter)
        {
            return withObservations(filter.build());
        }
        

        /**
         * Select only procedures with features of interest matching the filter
         * @param filter Features of interest filter
         * @return This builder for chaining
         */
        public B withFeaturesOfInterest(FoiFilter filter)
        {
            instance.foiFilter = filter;
            return (B)this;
        }
        

        /**
         * Select only procedures with features of interest matching the filter
         * @param filter Features of interest filter
         * @return This builder for chaining
         */
        public B withFeaturesOfInterest(FoiFilter.Builder filter)
        {
            return withFeaturesOfInterest(filter.build());
        }
    }
}
