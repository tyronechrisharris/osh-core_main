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
    private IdFilter memberOf;
    private IdFilter observedProperties;
    private ObsFilter observations;
    private FoiFilter featuresOfInterest; // shortcut for ObsFilter/FoiFilter
    
    
    /*
     * this class can only be instantiated using builder
     */
    private ProcedureFilter() {}
    
    
    public IdFilter getMemberOf()
    {
        return memberOf;
    }
    
    
    public IdFilter getObservedProperties()
    {
        return observedProperties;
    }
    
    
    public ObsFilter getObservationsFilter()
    {
        return observations;
    }


    public FoiFilter getFeaturesOfInterestFilter()
    {
        return featuresOfInterest;
    }
    
    
    public static Builder builder()
    {
        return new Builder();
    }
    
    
    public static class Builder extends FeatureFilter.Builder<Builder, ProcedureFilter>
    {        
        public Builder()
        {
            super(new ProcedureFilter());
        }
        
        
        public Builder withParentGroups(String... parentIds)
        {
            for (String id: parentIds)
                instance.memberOf.getIdList().add(id);
            return this;
        }
        
        
        public Builder withObservedProperties(String... observableIds)
        {
            for (String id: observableIds)
                instance.observedProperties.getIdList().add(id);
            return this;
        }


        public Builder withObservations(ObsFilter observations)
        {
            instance.observations = observations;
            return this;
        }


        public Builder withFeaturesOfInterest(FoiFilter featuresOfInterest)
        {
            instance.featuresOfInterest = featuresOfInterest;
            return this;
        }
    }
}
