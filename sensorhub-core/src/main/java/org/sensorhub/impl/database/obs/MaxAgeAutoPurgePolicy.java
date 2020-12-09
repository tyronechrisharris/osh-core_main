/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.database.obs;

import org.sensorhub.api.database.IProcedureObsDbAutoPurgePolicy;
import org.sensorhub.api.datastore.RangeFilter.RangeOp;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import java.time.Instant;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.slf4j.Logger;
import org.vast.util.DateTimeFormat;


/**
 * <p>
 * Implementation of purging policy removing records when they reach a 
 * certain age
 * </p>
 *
 * @author Alex Robin
 * @since Oct 29, 2019
 */
public class MaxAgeAutoPurgePolicy implements IProcedureObsDbAutoPurgePolicy
{
    MaxAgeAutoPurgeConfig config;
    DateTimeFormat df = new DateTimeFormat();
    
    
    MaxAgeAutoPurgePolicy(MaxAgeAutoPurgeConfig config)
    {
        this.config = config;
    }
    
    
    @Override
    public int trimStorage(IProcedureObsDatabase db, Logger log)
    {
        var oldestRecordTime = Instant.now().minusSeconds((long)config.maxRecordAge);
        
        db.getProcedureStore().removeEntries(new ProcedureFilter.Builder()
            .withValidTime(new TemporalFilter.Builder()
                .withOperator(RangeOp.CONTAINS)
                .withRange(Instant.MIN, oldestRecordTime)
                .build())
            .build());
        
        db.getFoiStore().removeEntries(new FoiFilter.Builder()
            .withValidTime(new TemporalFilter.Builder()
                .withOperator(RangeOp.CONTAINS)
                .withRange(Instant.MIN, oldestRecordTime)
                .build())
            .build());
                
        var numObsRemoved = (int)db.getObservationStore().removeEntries(new ObsFilter.Builder()
            .withResultTimeDuring(Instant.MIN, oldestRecordTime)
            .build());
                
        if (log.isInfoEnabled())
            log.info("Purging data up to {}. Removed {} observation records", oldestRecordTime, numObsRemoved);
        
        return numObsRemoved;
    }

}
