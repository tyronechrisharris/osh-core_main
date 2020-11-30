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

import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.impl.event.DelegatingSubscriberAdapter;
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
    
    
    // instead of letting replay threads wait while waiting for the next record,
    // use a pool of timers running at fixed frequency that can drive several
    // replay requests at once
    protected class StreamReplaySubscription extends StreamSubscription<IObsData> implements Subscription
    {
        double replaySpeedFactor;
        long requestStartTime;
        long requestSystemTime;
        ScheduledFuture<?> replayTimer;
        
        StreamReplaySubscription(Subscriber<IObsData> subscriber, int batchSize, Instant requestStartTime, double replaySpeedFactor, Stream<IObsData> itemStream)
        {
            super(subscriber, batchSize, itemStream);
            this.replaySpeedFactor = replaySpeedFactor;
            this.requestStartTime = requestStartTime.toEpochMilli();
        }

        @Override
        public void request(long n)
        {
            Asserts.checkArgument(n > 0);
            requested.addAndGet(n);
            
            if (replayTimer == null)
            {
                requestSystemTime = System.currentTimeMillis();
                
                replayTimer = threadPool.scheduleWithFixedDelay(() -> {
                    maybeSendItems();
                    maybeFetchFromStorage();
                }, 0, 10, TimeUnit.MILLISECONDS);
            }
        }
        
        @Override
        void maybeSendItems()
        {
            while (requested.get() > 0 && !itemQueue.isEmpty() && !canceled && !onCompleteCalled.get())
            {
                // slow down item dispatch at required replay speed
                var nextItem = itemQueue.peek();
                var deltaClockTime = (System.currentTimeMillis() - requestSystemTime) * replaySpeedFactor;
                var deltaObsTime = nextItem.getPhenomenonTime().toEpochMilli() - requestStartTime;
                //servlet.getLogger().debug("delta clock time = {}ms", deltaClockTime);
                //servlet.getLogger().debug("{} -> {}, delta={}ms", Instant.ofEpochMilli(requestStartTime), nextItem.getPhenomenonTime(), deltaObsTime);
                
                // skip if it's not time to send this record yet
                if (deltaObsTime > deltaClockTime)
                    return;
                
                //servlet.getLogger().debug("Sending record with ts="+ nextItem.getPhenomenonTime());
                subscriber.onNext(itemQueue.poll());
                requested.decrementAndGet();
            }
            
            maybeCallOnComplete();
        }

        @Override
        public void cancel()
        {
            super.cancel();
            if (replayTimer != null)
                replayTimer.cancel(false);
        }
    }
    
    
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
        // generate obs filter from request
        var obsFilter = getObsFilter(req, null);
        
        // cache of datastreams info
        var dataStreams = new HashMap<Long, DataStreamInfoCache>();
        
        // notify consumer with subscription
        consumer.onSubscribe(
            new StreamSubscription<>(
                consumer,
                100,
                database.getObservationStore().select(obsFilter)
                    .map(obs -> {
                        // get or compute result structure for this datastream
                        DataStreamInfoCache dsInfoCache = dataStreams.computeIfAbsent(obs.getDataStreamID(), k -> {
                            var dsInfo = database.getObservationStore().getDataStreams().get(new DataStreamKey(k));
                            return new DataStreamInfoCache(k, dsInfo);
                        });
                        
                        // can reuse same structure since we are running in a single thread
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
            // build equivalent GetObs request
            var getObsReq = new GetObservationRequest();
            getObsReq.getProcedures().add(procUID);
            getObsReq.getObservables().addAll(req.getObservables());
            getObsReq.getFoiIDs().addAll(req.getFoiIDs());
            getObsReq.setSpatialFilter(req.getSpatialFilter());
            getObsReq.setTemporalFilter(req.getTemporalFilter());
            var obsFilter = getObsFilter(getObsReq, selectedDataStream.internalId);
            
            // wrap consumer to map from IObsData to DataEvent
            var obsConsumer = new DelegatingSubscriberAdapter<IObsData, DataEvent>(consumer)
            {
                @Override
                public void onNext(IObsData item)
                {
                    consumer.onNext(toDataEvent(item));
                }                
            };
            
            // notify consumer with subscription            
            // if replay enable, use StreamReplaySubscription
            double replaySpeedFactor = SOSProviderUtils.getReplaySpeed(req);
            if (!Double.isNaN(replaySpeedFactor) && req.getTime() != null && req.getTime().hasBegin())
            {
                obsConsumer.onSubscribe(
                    new StreamReplaySubscription(obsConsumer, 100, req.getTime().begin(), replaySpeedFactor,
                        database.getObservationStore().select(obsFilter)) );
            }
            
            // else use regular StreamSubscription
            else
            {
                obsConsumer.onSubscribe(
                    new StreamSubscription<>(obsConsumer, 100,
                        database.getObservationStore().select(obsFilter)) );
            }
        }
        catch (SOSException e)
        {
            throw new CompletionException(e);
        }
    }
    
    
    protected DataEvent toDataEvent(IObsData obs)
    {
        return new DataEvent(
            obs.getResultTime().toEpochMilli(),
            selectedDataStream.procUID,
            selectedDataStream.resultStruct.getName(),
            obs.getFoiID().getUniqueID(),
            obs.getResult());
    }
}
