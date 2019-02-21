/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.

 Contributor(s): 
    Alexandre Robin "alex.robin@sensiasoft.com"
 
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
    
    
    public ObsFilter getObservations()
    {
        return observations;
    }


    public FoiFilter getFeaturesOfInterest()
    {
        return featuresOfInterest;
    }
    
    
    public static class Builder extends BaseBuilder<Builder, ProcedureFilter>
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
