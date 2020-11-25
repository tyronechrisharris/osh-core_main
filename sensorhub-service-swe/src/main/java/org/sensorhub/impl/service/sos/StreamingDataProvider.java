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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import net.opengis.gml.v32.AbstractGeometry;
import net.opengis.gml.v32.Envelope;
import net.opengis.swe.v20.DataArray;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataRecord;
import org.sensorhub.api.event.IEventBus;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IMultiSourceDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.IEventSource;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.impl.event.DelegatingSubscriber;
import org.sensorhub.impl.event.DelegatingSubscriberAdapter;
import org.sensorhub.impl.event.DelegatingSubscription;
import org.sensorhub.impl.service.swe.RecordTemplate;
import org.sensorhub.utils.DataStructureHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.data.DataIterator;
import org.vast.ogc.gml.GMLUtils;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.om.IObservation;
import org.vast.ogc.om.IProcedure;
import org.vast.ows.sos.GetFeatureOfInterestRequest;
import org.vast.ows.sos.GetObservationRequest;
import org.vast.ows.sos.GetResultRequest;
import org.vast.ows.sos.GetResultTemplateRequest;
import org.vast.ows.sos.SOSException;
import org.vast.ows.sos.SOSOfferingCapabilities;
import org.vast.ows.swe.DescribeSensorRequest;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
import org.vast.util.Bbox;
import org.vast.util.TimeExtent;


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
    IStreamingDataInterface selectedOutput;
    DataStructureHash resultStructureHash;


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
        // build equivalent GetResult request
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
                    IObservation obs = SOSProviderUtils.buildObservation(item.getProcedureID().getUniqueID(), item.getFoiUID(), result);
                    consumer.onNext(obs);
                }
            }
        });
    }


    @Override
    public void getResults(GetResultRequest req, Subscriber<DataEvent> consumer) throws SOSException
    {
        // TODO could improve performance here by wrapping everything into
        // a CompletableFuture.runAsync() call (i.e. even findOuput would run async)

        // find selected output
        findOutput(dataSource, config.excludedOutputs, req.getObservables());
        if (selectedOutput == null)
            throw new SOSException("Could not find output with observables " + req.getObservables());

        // build list of selected procedures
        if (dataSource instanceof IMultiSourceDataProducer)
        {
            IMultiSourceDataProducer multiProducer = (IMultiSourceDataProducer)dataSource;
            for (String foi: req.getFoiIDs())
            {
                for (ProcedureId procID: multiProducer.getProceduresWithFoi(foi))
                    procedureFois.put(procID.getUniqueID(), foi);
            }
        }
        else
        {
            IGeoFeature foi = dataSource.getCurrentFeatureOfInterest();
            String foiID = foi != null ? foi.getUniqueIdentifier() : null;
            procedureFois.put(dataSource.getUniqueIdentifier(), foiID);
        }

        // collect event sources from all procedure outputs
        Set<IEventSource> eventSources = new LinkedHashSet<>();
        getEventSources(dataSource, selectedOutput.getName(), req.getFoiIDs(), eventSources);

        // subscribe for data events only if continuous live stream was requested
        final TimeExtent timeRange = req.getTime() == null ? TimeExtent.allTimes() : req.getTime();
        if (!timeRange.isNow())
        {
            long timeOut = (long)(config.liveDataTimeout * 1000.);

            // prepare time indexer so we can check against request stop time
            ScalarIndexer timeIndexer;
            double stopTime;
            if (timeRange.hasEnd())
            {
                timeIndexer = SWEHelper.getTimeStampIndexer(selectedOutput.getRecordDescription());
                stopTime = timeRange.end().toEpochMilli() / 1000.0;
            }
            else
            {
                timeIndexer = null;
                stopTime = Double.POSITIVE_INFINITY;
            }

            // subscribe to event bus
            // wrap subscriber to handle timeout and end time
            eventBus.newSubscription(DataEvent.class)
                .withSources(eventSources)
                .withEventType(DataEvent.class)
                .subscribe(new DelegatingSubscriber<DataEvent>(consumer) {
                    Subscription sub;
                    boolean latestRecordsSent = false;
                    long lastRecordTimestamp;

                    @Override
                    public void onSubscribe(Subscription sub)
                    {
                        this.sub = sub;

                        // wrap subscription so we can send latest records before we actually
                        // start streaming real-time records from event bus
                        super.onSubscribe(new DelegatingSubscription(sub) {
                            @Override
                            public void request(long n)
                            {
                                // always send latest record of each producer (if available)
                                // synchronously before any other records
                                if (!latestRecordsSent)
                                {
                                    // TODO send only n records, not all of them
                                    latestRecordsSent = true;
                                    sendLatestRecords(eventSources, consumer);
                                }

                                super.request(n);
                            }
                        });

                        lastRecordTimestamp = System.currentTimeMillis();
                        timeOutMonitor.register(this::checkTimeOut);
                    }

                    protected void checkTimeOut()
                    {
                        if (System.currentTimeMillis() - lastRecordTimestamp > timeOut)
                        {
                            sub.cancel();
                            onComplete();
                        }
                    }

                    @Override
                    public void onNext(DataEvent item)
                    {
                        lastRecordTimestamp = item.getTimeStamp();
                        DataBlock[] data = item.getRecords();
                        if (timeIndexer != null && timeIndexer.getDoubleValue(data[data.length-1]) > stopTime)
                            super.onComplete();
                        else
                            super.onNext(item);
                    }
                });
        }

        // otherwise just send latest records synchronously
        else
        {
            consumer.onSubscribe(new Subscription() {
                boolean latestRecordsSent = false;
                @Override
                public void request(long n)
                {
                    if (!latestRecordsSent)
                    {
                        // TODO send only n records, not all of them
                        latestRecordsSent = true;
                        sendLatestRecords(eventSources, consumer);
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
     * Find the output corresponding to the selected observables
     * TODO support selecting multiple outputs since it is possible with GetObservation
     */
    protected IStreamingDataInterface findOutput(IDataProducer producer, List<String> excludedOutputs, Set<String> defUris)
    {
        // return the output we already found
        if (selectedOutput != null)
            return selectedOutput;

        for (IStreamingDataInterface output: producer.getOutputs().values())
        {
            // skip hidden outputs
            if (excludedOutputs != null && excludedOutputs.contains(output.getName()))
                continue;

            // select it if we can find one of the observables or any observable is accepted
            if (!defUris.isEmpty())
            {
                DataIterator it = new DataIterator(output.getRecordDescription());
                while (it.hasNext() && selectedOutput == null)
                {
                    String defUri = it.next().getDefinition();
                    if (defUris.contains(defUri))
                        selectedOutput = output;
                }
            }
            else
                selectedOutput = output;

            if (selectedOutput != null)
            {
                resultStructureHash = new DataStructureHash(selectedOutput.getRecordDescription());
                log.debug("Selected output: {}", selectedOutput.getName());
                return selectedOutput;
            }
        }

        // if multi producer, try to find output in any of the nested producers
        if (selectedOutput == null && producer instanceof IMultiSourceDataProducer)
        {
            IMultiSourceDataProducer multiSource = (IMultiSourceDataProducer)producer;
            for (IDataProducer member: multiSource.getMembers().values())
            {
                // return the first one we find
                IStreamingDataInterface output = findOutput(member, excludedOutputs, defUris);
                if (output != null)
                    return output;
            }
        }

        return null;
    }


    /*
     * Recursively get output event sources from the main producer
     * and all selected sub-producers if it is a group
     */
    protected void getEventSources(IDataProducer dataSource, String outputName, Set<String> foiIDs, Set<IEventSource> eventSources)
    {
        if (dataSource instanceof IMultiSourceDataProducer)
        {
            IMultiSourceDataProducer multiProducer = (IMultiSourceDataProducer)dataSource;

            if (foiIDs != null && !foiIDs.isEmpty())
            {
                for (String foiID: foiIDs)
                {
                    for (ProcedureId procID: multiProducer.getProceduresWithFoi(foiID))
                    {
                        procedureFois.put(procID.getUniqueID(), foiID);
                        IDataProducer proc = multiProducer.getMembers().get(procID);
                        getEventSources(proc, outputName, foiIDs, eventSources);
                    }
                }
            }
            else
            {
                for (IDataProducer proc: multiProducer.getMembers().values())
                {
                    IGeoFeature foi = proc.getCurrentFeatureOfInterest();
                    String foiUID = foi == null ? null : foi.getUniqueIdentifier();
                    procedureFois.put(proc.getUniqueIdentifier(), foiUID);
                    getEventSources(proc, outputName, foiIDs, eventSources);
                }
            }
        }
        else
        {
            // get selected output
            IStreamingDataInterface output = dataSource.getOutputs().get(outputName);
            if (output == null)
                return;

            // only use output if structure is compatible with selected output
            // needed in case there is an output with the same name but different structure
            if (!resultStructureHash.equals(new DataStructureHash(output.getRecordDescription())))
                return;

            log.debug("Selected data source: {}", output.getEventSourceInfo());
            eventSources.add(output);
        }
    }


    /*
     * Send the latest record of each data source to the consumer
     */
    protected void sendLatestRecords(Set<IEventSource> eventSources, Subscriber<DataEvent> consumer)
    {
        for (IEventSource eventSrc: eventSources)
        {
            IStreamingDataInterface output = (IStreamingDataInterface)eventSrc;
            DataBlock data = output.getLatestRecord();
            if (data != null)
                consumer.onNext(new DataEvent(System.currentTimeMillis(), output, data));
        }
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
