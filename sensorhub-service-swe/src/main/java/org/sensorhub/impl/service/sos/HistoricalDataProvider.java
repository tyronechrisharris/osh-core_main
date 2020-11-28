/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.util.HashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscriber;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.vast.ogc.om.IObservation;
import org.vast.ows.sos.GetObservationRequest;
import org.vast.ows.sos.GetResultRequest;
import org.vast.ows.sos.SOSException;
import org.vast.util.Asserts;


/**
 * <p>
 * Implementation of SOS data provider used to retrieve historical observations
 * from a database using OSH datastore API
 * </p>
 *
 * @author Alex Robin
 * @since April 15, 2020
 */
public class HistoricalDataProvider extends ProcedureDataProvider
{
    private static final String TOO_MANY_OBS_MSG = "Too many observations requested. Please further restrict your filtering options";

    
    public HistoricalDataProvider(final SOSService service, final ProcedureDataProviderConfig config)
    {
        super(service.getServlet(),
             service.getReadDatabase(),
             service.getThreadPool(),
             config);
    }


    @Override
    public void getObservations(GetObservationRequest req, Subscriber<IObservation> consumer) throws SOSException
    {
        // build equivalent GetResult request
        var grReq = new GetResultRequest();
        grReq.getProcedures().addAll(req.getProcedures());
        grReq.getObservables().addAll(req.getObservables());
        grReq.getFoiIDs().addAll(req.getFoiIDs());
        grReq.setSpatialFilter(req.getSpatialFilter());
        grReq.setTemporalFilter(req.getTemporalFilter());
        var obsFilter = getObsFilter(grReq, null);
        
        // cache of result structure
        var resultStructs = new HashMap<Long, DataStreamInfoCache>();
        
        // notify consumer with subscription
        consumer.onSubscribe(
            new StreamSubscription<>(
                consumer,
                100,
                database.getObservationStore().select(obsFilter)
                    .map(obs -> {
                        // get or compute result structure for this datastream
                        DataStreamInfoCache dsInfoCache = resultStructs.computeIfAbsent(obs.getDataStreamID(), k -> {
                            var dsInfo = database.getObservationStore().getDataStreams().get(new DataStreamKey(k));
                            return new DataStreamInfoCache(k, dsInfo);
                        });
                        
                        var result = dsInfoCache.resultStruct;
                        result.setData(obs.getResult());
                        return SOSProviderUtils.buildObservation(dsInfoCache.procUID, obs.getFoiID().getUniqueID(), result);
                    })
            )
        );
    }


    @Override
    public void getResults(GetResultRequest req, Subscriber<DataEvent> consumer) throws SOSException
    {
        Asserts.checkState(selectedDataStream != null, "getResultTemplate hasn't been called");
        String procUID = getProcedureUID(req.getOffering());
        
        try
        {
            var obsFilter = getObsFilter(req, selectedDataStream.internalId);
            
            // notify consumer with subscription
            consumer.onSubscribe(
                new StreamSubscription<>(
                    consumer,
                    100,
                    database.getObservationStore().select(obsFilter)
                        .map(obs -> new DataEvent(
                            obs.getResultTime().toEpochMilli(),
                            procUID,
                            selectedDataStream.resultStruct.getName(),
                            obs.getFoiID().getUniqueID(),
                            obs.getResult())
                        )
                    )
                );
        }
        catch (SOSException e)
        {
            throw new CompletionException(e);
        }
    }
    

    /*@Override
    public DataBlock getNextResultRecord() throws IOException
    {
        if (!obsIterator.hasNext())
            return null;
        
        lastObs = obsIterator.next();
        long obsTime = lastObs.getPhenomenonTime().toEpochMilli();
        if (requestStartTime == Long.MIN_VALUE)
            requestStartTime = obsTime;
        
        // wait if replay mode is active
        if (!Double.isNaN(replaySpeedFactor))
        {
            long realEllapsedTime = System.currentTimeMillis() - requestSystemTime;
            
            long waitTime = (long)((obsTime - requestStartTime) * 1000. / replaySpeedFactor) - realEllapsedTime;
            if (waitTime > 0)
            {
                try { Thread.sleep(waitTime ); }
                catch (InterruptedException e) { }
            }
        }
        
        return lastObs.getResult();
    }*/

}
