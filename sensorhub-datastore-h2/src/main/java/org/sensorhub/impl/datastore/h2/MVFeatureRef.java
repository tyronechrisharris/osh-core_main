/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.Instant;
import org.sensorhub.utils.ObjectUtils;
import com.google.common.collect.Range;


/**
 * <p>
 * Internal object stored in secondary indexes to reference a feature
 * </p>
 *
 * @author Alex Robin
 * @date Apr 12, 2018
 */
public class MVFeatureRef
{
    private long internalID;
    private Range<Instant> validityPeriod;
    //private Geometry geom;


    public long getInternalID()
    {
        return internalID;
    }


    /*public Geometry getGeom()
    {
        return geom;
    }*/


    public Range<Instant> getValidityPeriod()
    {
        return validityPeriod;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
    
    
    public static class Builder
    {
        private MVFeatureRef instance = new MVFeatureRef();
        
        
        public Builder withInternalID(long internalID)
        {
            instance.internalID = internalID;
            return this;
        }
        
        
        /*public Builder withGeom(Geometry geom)
        {
            instance.geom = geom;
            return this;
        }*/
        
        
        public Builder withValidityPeriod(Range<Instant> validityPeriod)
        {
            instance.validityPeriod = validityPeriod;
            return this;
        }
        
        
        public MVFeatureRef build()
        {
            return instance;
        }
    }
    
}
