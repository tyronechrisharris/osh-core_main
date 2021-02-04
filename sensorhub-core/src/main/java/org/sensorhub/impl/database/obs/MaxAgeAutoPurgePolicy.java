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
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
    public void trimStorage(IProcedureObsDatabase db, Logger log)
    {
        // remove all procedures, datastreams and fois whose validity time period
        // ended before (now - max age)        
        var oldestRecordTime = Instant.now().minusSeconds((long)config.maxRecordAge);
        
        long numProcRemoved = db.getProcedureStore().removeEntries(new ProcedureFilter.Builder()
            .withValidTime(new TemporalFilter.Builder()
                .withOperator(RangeOp.CONTAINS)
                .withRange(Instant.MIN, oldestRecordTime)
                .build())
            .build());
        
        long numFoisRemoved = db.getFoiStore().removeEntries(new FoiFilter.Builder()
            .withValidTime(new TemporalFilter.Builder()
                .withOperator(RangeOp.CONTAINS)
                .withRange(Instant.MIN, oldestRecordTime)
                .build())
            .build());
        
        long numDsRemoved = db.getDataStreamStore().removeEntries(new DataStreamFilter.Builder()
            .withValidTime(new TemporalFilter.Builder()
                .withOperator(RangeOp.CONTAINS)
                .withRange(Instant.MIN, oldestRecordTime)
                .build())
            .build());
        
        // for each remaining datastream, remove all obs with a timestamp older than
        // the latest result time minus the max age
        long numObsRemoved = 0;
        var allDataStreams = db.getDataStreamStore().selectEntries(db.getDataStreamStore().selectAllFilter()).iterator();
        while (allDataStreams.hasNext())
        {
            var dsEntry = allDataStreams.next();
            var dsID = dsEntry.getKey().getInternalID();
            var resultTimeRange = dsEntry.getValue().getResultTimeRange();
            
            if (resultTimeRange != null)
            {            
                var oldestResultTime = resultTimeRange.end().minusSeconds((long)config.maxRecordAge);
                numObsRemoved += db.getObservationStore().removeEntries(new ObsFilter.Builder()
                    .withDataStreams(dsID)
                    .withResultTimeDuring(Instant.MIN, oldestResultTime)
                    .build());
            }
        }
                
        if (log.isInfoEnabled())
        {
            log.info("Purging data until {}. Removed records: {} procedures, {} fois, {} datastreams, {} observations",
                oldestRecordTime.truncatedTo(ChronoUnit.SECONDS),
                numProcRemoved, numFoisRemoved, numDsRemoved, numObsRemoved);
        }
    }

}
