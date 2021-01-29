/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
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
    private long procedureID = 0;
    private Range<Instant> phenomenonTimeRange = null;
    
    
    /*
     * this class can only be instantiated using builder
     */
    FoiPeriod()
    {
    }
    
    
    /**
     * @return The internal ID of the procedure that produced the observations
     * or the constant {@link #ALL_PROCEDURES} if this cluster represents
     * observations from all procedures.
     */
    public long getProcedureID()
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
