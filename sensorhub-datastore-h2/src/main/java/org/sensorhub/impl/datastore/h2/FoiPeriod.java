/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.Instant;
import com.google.common.collect.Range;


/**
 * <p>
 * TODO FoiPeriod type description
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class FoiPeriod
{
    private String procedureID = null;
    private Range<Instant> phenomenonTimeRange = null;
    
    
    /*
     * this class can only be instantiated using builder
     */
    FoiPeriod()
    {
    }
    
    
    /**
     * @return The unique ID of the procedure that produced the observations or
     * the constant {@link #ALL_PROCEDURES} if this cluster represents
     * observations from all procedures.
     */
    public String getProcedureID()
    {
        return procedureID;
    }
    
    
    /**
     * @return The range of phenomenon times for this FOi observations
     */
    public Range<Instant> getPhenomenonTimeRange()
    {
        return phenomenonTimeRange;
    }
}
