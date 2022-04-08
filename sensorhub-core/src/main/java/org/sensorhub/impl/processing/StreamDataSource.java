/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.processing;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Consumer;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.data.ObsEvent;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.processing.OSHProcessInfo;
import org.vast.process.ExecutableProcessImpl;
import org.vast.process.ProcessException;
import org.vast.process.ProcessInfo;
import org.vast.swe.SWEHelper;
import net.opengis.swe.v20.Text;


/**
 * <p>
 * Process implementation used to feed data from any real-time datasource
 * (received from event bus) into a SensorML processing chain.
 * </p>
 *
 * @author Alex Robin
 * @since July 12, 2017
 */
public class StreamDataSource extends ExecutableProcessImpl implements ISensorHubProcess
{
    public static final OSHProcessInfo INFO = new OSHProcessInfo("datasource:stream", "Stream Data Source", null, StreamDataSource.class);
    public static final String PRODUCER_URI_PARAM = "producerURI";
    
    ISensorHub hub;
    Text producerUriParam;
    
    String producerUri;
    boolean paused;
    Subscription sub;
    
    
    public StreamDataSource()
    {
        super(INFO);
        SWEHelper fac = new SWEHelper();
        
        // param
        producerUriParam = fac.createText()
            .definition(SWEHelper.getPropertyUri("ProducerUID"))
            .label("Producer Unique ID")
            .build();
        paramData.add(PRODUCER_URI_PARAM, producerUriParam);
        
        // output cannot be created until source URI param is set
    }


    @Override
    public void notifyParamChange()
    {
        producerUri = producerUriParam.getData().getStringValue();
        
        if (producerUri != null)
        {
            var db = hub.getDatabaseRegistry().getFederatedDatabase();
            var procEntry = db.getSystemDescStore().getCurrentVersionEntry(producerUri);
            if (procEntry == null)
                throw new IllegalStateException("System with URI " + producerUri + " not found");
            
            // set process info
            ProcessInfo instanceInfo = new ProcessInfo(
                    processInfo.getUri(),
                    procEntry.getValue().getName(),
                    processInfo.getDescription(),
                    processInfo.getImplementationClass());
            this.processInfo = instanceInfo;
            
            // add outputs
            outputData.clear();            
            db.getDataStreamStore().select(new DataStreamFilter.Builder()
                    .withSystems(procEntry.getKey().getInternalID())
                    .withCurrentVersion()
                    .build())
                .forEach(ds -> {
                    outputData.add(ds.getOutputName(), ds.getRecordStructure().copy());
                });
        }
    }
    
    
    @Override
    public synchronized void start(Consumer<Throwable> onError) throws ProcessException
    {
        start(null, onError);
    }
    
    
    @Override
    public void start(ExecutorService threadPool, Consumer<Throwable> onError) throws ProcessException
    {
        if (!started)
        {
            started = true;
            
            var topics = new ArrayList<String>();
            for (var output: outputData.getProperties())
                topics.add(EventUtils.getDataStreamDataTopicID(producerUri, output.getName()));
            
            hub.getEventBus().newSubscription(ObsEvent.class)
            .withTopicIDs(topics)
            .subscribe(new Subscriber<ObsEvent>() {

                @Override
                public void onSubscribe(Subscription subscription)
                {
                    sub = subscription;
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ObsEvent e)
                {
                    if (paused)
                        return;
                    
                    String outputName = e.getOutputName();
                    var output = outputData.getComponent(outputName);
                    
                    // process each data block
                    for (var obs: e.getObservations())
                    {
                        try
                        {
                            output.setData(obs.getResult());
                            publishData(outputName);
                        }
                        catch (InterruptedException ex)
                        {
                            Thread.currentThread().interrupt();
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable)
                {
                    getLogger().error("Error receiving data from event bus", throwable);
                    
                    if (onError != null)
                        onError.accept(throwable);
                }

                @Override
                public void onComplete()
                {
                }
            });
            
            getLogger().debug("Connected to data source '{}'", producerUri);
        }
    }


    @Override
    public synchronized void stop()
    {
        if (started)
        {
            started = false;
            sub.cancel();
            getLogger().debug("Disconnected from data source '{}'", producerUri);
        }
    }


    public void pause()
    {
        this.paused = true;
    }


    public void resume()
    {
        this.paused = false;
    }


    @Override
    public void execute() throws ProcessException
    {
        // nothing to do, data is just pushed to outputs
        // when events are received
    }


    @Override
    public boolean needSync()
    {
        return false;
    }


    @Override
    public void setParentHub(ISensorHub hub)
    {
        this.hub = hub;        
    }

}
