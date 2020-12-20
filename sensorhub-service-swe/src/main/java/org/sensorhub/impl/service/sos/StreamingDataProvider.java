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
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import net.opengis.swe.v20.DataBlock;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.event.DelegatingSubscriber;
import org.sensorhub.impl.event.DelegatingSubscriberAdapter;
import org.sensorhub.impl.event.DelegatingSubscription;
import org.vast.ogc.om.IObservation;
import org.vast.ows.sos.GetObservationRequest;
import org.vast.ows.sos.GetResultRequest;
import org.vast.ows.sos.SOSException;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.collect.ImmutableMap;


/**
 * <p>
 * Implementation of SOS data provider used to stream real-time observations
 * from a procedure using the event-bus.
 * </p>
 *
 * @author Alex Robin
 * @since Apr 10, 2020
 */
public class StreamingDataProvider extends ProcedureDataProvider
{
    final IEventBus eventBus;
    final TimeOutMonitor timeOutMonitor;


    public StreamingDataProvider(final SOSService service, final ProcedureDataProviderConfig config)
    {
        super(service.getServlet(),
             service.getReadDatabase(),
             service.getThreadPool(),
             config);
        
        this.eventBus = service.getParentHub().getEventBus();
        this.timeOutMonitor = Asserts.checkNotNull(service.getTimeOutMonitor(), TimeOutMonitor.class);
    }


    @Override
    public void getObservations(GetObservationRequest req, Subscriber<IObservation> consumer) throws SOSException
    {
        var originalTimeFilter = req.getTime();
        
        // build obs filter from request but using 'now' for time
        req.setTime(TimeExtent.now());
        var obsFilter = getObsFilter(req, null);
        
        // cache of datastreams info
        var dataStreams = new HashMap<Long, DataStreamInfoCache>();
        var dataStreamsBySource = new HashMap<String, DataStreamInfoCache>();
        
        // query selected datastreams
        var dsFilter = obsFilter.getDataStreamFilter();
        database.getDataStreamStore().selectEntries(dsFilter)
            .forEach(dsEntry -> {
                var dsKey = dsEntry.getKey();
                var dsInfo = dsEntry.getValue();
                var dsInfoCache = new DataStreamInfoCache(dsKey.getInternalID(), dsInfo);
                dataStreams.put(dsInfoCache.internalId, dsInfoCache);
                var eventSrc = EventUtils.getProcedureOutputSourceID(
                    dsInfoCache.procUID,
                    dsInfoCache.resultStruct.getName());
                dataStreamsBySource.put(eventSrc, dsInfoCache);
            });

        // call getResults and transform DataEvents to Observation objects
        subscribeAndProcessDataEvents(dataStreams, originalTimeFilter, obsFilter, new DelegatingSubscriberAdapter<DataEvent, IObservation>(consumer) {
            public void onNext(DataEvent e)
            {
                var dsInfoCache = dataStreamsBySource.get(e.getSourceID());
                
                for (DataBlock data: e.getRecords())
                {
                    // can reuse same structure since we are running in a single thread
                    var result = dsInfoCache.resultStruct;
                    result.setData(data);
                    consumer.onNext(SOSProviderUtils.buildObservation(dsInfoCache.procUID, e.getFoiUID(), result));
                }
            }
        });
    }


    @Override
    public void getResults(GetResultRequest req, Subscriber<DataEvent> consumer) throws SOSException
    {
        Asserts.checkState(selectedDataStream != null, "getResultTemplate hasn't been called");
        String procUID = getProcedureUID(req.getOffering());
                
        // build obs filter for current records (i.e. 'now')
        // Use equivalent GetObs request
        var getObsReq = new GetObservationRequest();
        getObsReq.getProcedures().add(procUID);
        getObsReq.getObservables().addAll(req.getObservables());
        getObsReq.getFoiIDs().addAll(req.getFoiIDs());
        getObsReq.setSpatialFilter(req.getSpatialFilter());
        getObsReq.setTemporalFilter(req.getTemporalFilter());
        getObsReq.setTime(TimeExtent.now());
        var obsFilter = getObsFilter(getObsReq, selectedDataStream.internalId);
        
        // select a single datastream
        var dataStreams = ImmutableMap.of(selectedDataStream.internalId, selectedDataStream);
        
        // subscribe to event bus and forward all matching events
        subscribeAndProcessDataEvents(dataStreams, req.getTime(), obsFilter, consumer);
    }
    
    
    protected void subscribeAndProcessDataEvents(Map<Long, DataStreamInfoCache> dataStreams, TimeExtent timeFilter, ObsFilter obsFilter, Subscriber<DataEvent> consumer)
    {        
        // create set of event sources
        var eventSources = new HashSet<String>();
        for (var dsInfoCache: dataStreams.values())
        {
            var eventSrc = EventUtils.getProcedureOutputSourceID(
                dsInfoCache.procUID,
                dsInfoCache.resultStruct.getName());
            eventSources.add(eventSrc);
        }        
        
        // subscribe for data events only if continuous live stream was requested
        if (timeFilter != null && !timeFilter.isNow())
        {
            long timeOut = (long)(config.liveDataTimeout * 1000.);

            // prepare time indexer so we can check against request stop time
            // this only works for GetResult for now
            ScalarIndexer timeIndexer;
            double stopTime;
            if (timeFilter.hasEnd() && selectedDataStream != null)
            {
                timeIndexer = SWEHelper.getTimeStampIndexer(selectedDataStream.resultStruct);
                stopTime = timeFilter.end().toEpochMilli() / 1000.0;
            }
            else
            {
                timeIndexer = null;
                stopTime = Double.POSITIVE_INFINITY;
            }

            // subscribe to event bus
            // wrap subscriber to handle timeout and end time
            eventBus.newSubscription(DataEvent.class)
                .withTopicIDs(eventSources)
                .withEventType(DataEvent.class)
                .subscribe(new DelegatingSubscriber<DataEvent>(consumer) {
                    Subscription wrappedSub;
                    boolean currentTimeRecordsSent = false;
                    volatile boolean canceled = false;
                    long latestRecordTimestamp;

                    @Override
                    public void onSubscribe(Subscription sub)
                    {
                        latestRecordTimestamp = System.currentTimeMillis();
                        
                        // wrap subscription so we can send latest records before we actually
                        // start streaming real-time records from event bus
                        var delegatingSubscriber = this;
                        this.wrappedSub = new DelegatingSubscription(sub) {
                            @Override
                            public void request(long n)
                            {
                                // always send current time record of each producer (if available)
                                // synchronously if available
                                if (!currentTimeRecordsSent)
                                {
                                    // TODO send only n records, not all of them
                                    currentTimeRecordsSent = true;
                                    sendLatestRecords(dataStreams, obsFilter, delegatingSubscriber);
                                    
                                    // stop here if there was nothing since timeout period
                                    if (System.currentTimeMillis() - latestRecordTimestamp > timeOut)
                                    {
                                        onComplete();
                                        return;
                                    }
                                    
                                    timeOutMonitor.register(delegatingSubscriber::checkTimeOut);
                                }
                                
                                super.request(n);
                            }

                            @Override
                            public void cancel()
                            {
                                servlet.getLogger().debug("Canceling subscription: " + eventSources);
                                super.cancel();
                                canceled = true;
                            }
                        };
            
                        super.onSubscribe(wrappedSub);
                    }

                    protected boolean checkTimeOut()
                    {
                        if (canceled)
                            return true;
                        
                        if (System.currentTimeMillis() - latestRecordTimestamp > timeOut)
                        {
                            servlet.getLogger().debug("Data provider timeout");
                            wrappedSub.cancel();
                            onComplete();
                            return true;
                        }
                        
                        return false;
                    }

                    @Override
                    public void onNext(DataEvent item)
                    {
                        latestRecordTimestamp = item.getTimeStamp();
                        //servlet.getLogger().debug("Event ts={}", latestRecordTimestamp);
                        DataBlock[] data = item.getRecords();
                        if (timeIndexer != null && timeIndexer.getDoubleValue(data[data.length-1]) > stopTime)
                            onComplete();
                        if (acceptEvent(obsFilter, item))
                            super.onNext(item);
                        else
                            wrappedSub.request(1);
                    }
                });
        }
        else
            sendLatestRecordsOnly(dataStreams, obsFilter, consumer);
    }
    
    
    protected boolean acceptEvent(ObsFilter filter, DataEvent e)
    {
        var foiFilter = filter.getFoiFilter();
        if (foiFilter != null && foiFilter.getUniqueIDs() != null)
        {
            if (e.getFoiUID() == null || !foiFilter.getUniqueIDs().contains(e.getFoiUID()))
                return false;
        }
        
        return true;        
    }
    
    
    protected void sendLatestRecordsOnly(Map<Long, DataStreamInfoCache> dataStreams, ObsFilter obsFilter, Subscriber<DataEvent> consumer)
    {
        consumer.onSubscribe(new Subscription() {
            boolean currentTimeRecordsSent = false;
            
            @Override
            public void request(long n)
            {
                if (!currentTimeRecordsSent)
                {
                    // TODO send only n records, not all of them
                    currentTimeRecordsSent = true;
                    sendLatestRecords(dataStreams, obsFilter, consumer);
                    consumer.onComplete();
                }
            }

            @Override
            public void cancel()
            {
            }
        });
    }


    /*
     * Send the latest record of each data source to the consumer
     */
    protected void sendLatestRecords(Map<Long, DataStreamInfoCache> dataStreams, ObsFilter obsFilter, Subscriber<DataEvent> consumer)
    {
        database.getObservationStore().select(obsFilter)
            .forEach(obs -> {
                var dsInfoCache = dataStreams.get(obs.getDataStreamID());
                consumer.onNext(new DataEvent(
                    obs.getResultTime().toEpochMilli(),
                    dsInfoCache.procUID,
                    dsInfoCache.resultStruct.getName(),
                    obs.getFoiID().getUniqueID(),
                    obs.getResult()));
            });
    }


    @Override
    public boolean hasMultipleProducers()
    {
        return false;
    }


    @Override
    public void close()
    {

    }
}
