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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import net.opengis.swe.v20.DataBlock;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.event.DelegatingSubscriber;
import org.sensorhub.impl.event.DelegatingSubscription;
import org.vast.ogc.om.IObservation;
import org.vast.ows.sos.GetObservationRequest;
import org.vast.ows.sos.GetResultRequest;
import org.vast.ows.sos.SOSException;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import com.google.common.collect.ImmutableSet;


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
    final Map<String, String> procedureFois;


    public StreamingDataProvider(final SOSService service, final ProcedureDataProviderConfig config)
    {
        super(service.getServlet(),
             service.getReadDatabase(),
             service.getThreadPool(),
             config);
        
        this.eventBus = service.getParentHub().getEventBus();
        this.timeOutMonitor = Asserts.checkNotNull(service.getTimeOutMonitor(), TimeOutMonitor.class);
        this.procedureFois = new TreeMap<>();
    }


    @Override
    public void getObservations(GetObservationRequest req, Subscriber<IObservation> consumer) throws SOSException
    {
        /*// build equivalent GetResult request
        GetResultRequest grReq = new GetResultRequest();
        grReq.getObservables().addAll(req.getObservables());
        grReq.getFoiIDs().addAll(req.getFoiIDs());
        grReq.setTemporalFilter(req.getTemporalFilter());

        // call getResults and transform DataEvents to Observation objects
        getResults(grReq, new DelegatingSubscriberAdapter<DataEvent, IObservation>(consumer) {
            @Override
            public void onNext(DataEvent item)
            {
                for (DataBlock data: item.getRecords())
                {
                    // transform each record into a separate observation
                    DataComponent result = selectedOutput.getRecordDescription().copy();
                    result.setData(data);
                    IObservation obs = SOSProviderUtils.buildObservation(item.getProcedureUID(), item.getFoiUID(), result);
                    consumer.onNext(obs);
                }
            }
        });*/
    }


    @Override
    public void getResults(GetResultRequest req, Subscriber<DataEvent> consumer) throws SOSException
    {
        Asserts.checkState(selectedDataStream != null, "getResultTemplate hasn't been called");
        String procUID = getProcedureUID(req.getOffering());
        
        // generate obs filter for current records
        var timeFilter = req.getTime();
        req.setTime(TimeExtent.now());
        var obsFilter = getObsFilter(req, selectedDataStream.internalId);
        
        // select all event sources
        var eventSrc = EventUtils.getProcedureOutputSourceID(
            selectedDataStream.procUID,
            selectedDataStream.resultStruct.getName());
        var eventSources = ImmutableSet.of(eventSrc);
        
        // subscribe for data events only if continuous live stream was requested
        if (timeFilter != null && !timeFilter.isNow())
        {
            long timeOut = (long)(config.liveDataTimeout * 1000.);

            // prepare time indexer so we can check against request stop time
            ScalarIndexer timeIndexer;
            double stopTime;
            if (timeFilter.hasEnd())
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
                                    sendLatestRecords(obsFilter, delegatingSubscriber);
                                    
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
                        servlet.getLogger().debug("Event ts={}", latestRecordTimestamp);
                        DataBlock[] data = item.getRecords();
                        if (timeIndexer != null && timeIndexer.getDoubleValue(data[data.length-1]) > stopTime)
                            onComplete();
                        else if (req.getFoiIDs().isEmpty() || req.getFoiIDs().contains(item.getFoiUID()))
                            super.onNext(item);
                    }
                });
        }

        // otherwise just send current time records synchronously
        else
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
                        sendLatestRecords(obsFilter, consumer);
                        consumer.onComplete();
                    }
                }

                @Override
                public void cancel()
                {
                }
            });
        }  
    }


    /*
     * Send the latest record of each data source to the consumer
     */
    protected void sendLatestRecords(ObsFilter obsFilter, Subscriber<DataEvent> consumer)
    {
        database.getObservationStore().select(obsFilter)
            .forEach(obs -> {
                consumer.onNext(new DataEvent(
                    obs.getResultTime().toEpochMilli(),
                    selectedDataStream.procUID,
                    selectedDataStream.resultStruct.getName(),
                    obs.getFoiID().getUniqueID(),
                    obs.getResult()));
            });
    }


    @Override
    public boolean hasMultipleProducers()
    {
        return procedureFois.size() > 1;
    }


    @Override
    public void close()
    {

    }
}
