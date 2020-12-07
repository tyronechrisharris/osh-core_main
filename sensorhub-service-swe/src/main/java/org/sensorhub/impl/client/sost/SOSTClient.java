/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.client.sost;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import net.opengis.swe.v20.DataBlock;
import org.sensorhub.api.client.ClientException;
import org.sensorhub.api.client.IClientModule;
import org.sensorhub.api.event.EventUtils;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.procedure.ProcedureFilter;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.obs.DataStreamAddedEvent;
import org.sensorhub.api.obs.DataStreamDisabledEvent;
import org.sensorhub.api.obs.DataStreamEnabledEvent;
import org.sensorhub.api.obs.DataStreamRemovedEvent;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.obs.IObsData;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.procedure.ProcedureDisabledEvent;
import org.sensorhub.api.procedure.ProcedureEnabledEvent;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.impl.comm.RobustIPConnection;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.impl.module.RobustConnection;
import org.sensorhub.impl.security.ClientAuth;
import org.sensorhub.utils.SerialExecutor;
import org.sensorhub.utils.StreamException;
import org.vast.cdm.common.DataStreamWriter;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.om.IObservation;
import org.vast.ogc.om.ObservationImpl;
import org.vast.ows.GetCapabilitiesRequest;
import org.vast.ows.OWSException;
import org.vast.ows.sos.InsertResultRequest;
import org.vast.ows.sos.InsertResultTemplateRequest;
import org.vast.ows.sos.InsertResultTemplateResponse;
import org.vast.ows.sos.InsertSensorRequest;
import org.vast.ows.sos.SOSInsertionCapabilities;
import org.vast.ows.sos.SOSServiceCapabilities;
import org.vast.ows.sos.SOSUtils;
import org.vast.ows.swe.InsertSensorResponse;
import org.vast.ows.swe.SWESUtils;
import org.vast.ows.swe.UpdateSensorRequest;
import org.vast.swe.Base64Encoder;
import org.vast.swe.SWEData;


/**
 * <p>
 * Implementation of an SOS-T client that listens to procedure events and 
 * forwards them to a remote SOS via InsertSensor/UpdateSensor,
 * InsertResultTemplate and InsertResult requests.<br/>
 * </p>
 *
 * @author Alex Robin
 * @since Feb 6, 2015
 */
public class SOSTClient extends AbstractModule<SOSTClientConfig> implements IClientModule<SOSTClientConfig>
{
    RobustConnection connection;
    IProcedureObsDatabase dataBaseView;
    SOSUtils sosUtils = new SOSUtils();  
    String sosEndpointUrl;
    Subscription registrySub;
    Map<String, ProcedureInfo> procedures;
    NavigableMap<String, StreamInfo> dataStreams;
    ExecutorService threadPool = ForkJoinPool.commonPool();
    
    
    public class ProcedureInfo
    {
        //private long internalID;
        private String offeringId;
        private Subscription sub;
        private Instant startValidTime;
    }
    
    
    public class StreamInfo
    {
        public long lastEventTime = Long.MIN_VALUE;
        public int measPeriodMs = 1000;
        public int errorCount = 0;
        private int minRecordsPerRequest = 10;
        private long internalID;
        private String procUID;
        private String outputName;
        private String eventSrcId;
        private Subscription sub;
        private String templateId;
        private SWEData resultData = new SWEData();
        private SerialExecutor executor = new SerialExecutor(threadPool, config.connection.maxQueueSize);
        private HttpURLConnection connection;
        private DataStreamWriter persistentWriter;
        private volatile boolean connecting = false;
        private volatile boolean stopping = false;
    }
    
    
    public SOSTClient()
    {
        this.startAsync = true;
        this.procedures = new ConcurrentSkipListMap<>();
        this.dataStreams = new ConcurrentSkipListMap<>();
    }
    
    
    private void setAuth()
    {
        ClientAuth.getInstance().setUser(config.sos.user);
        if (config.sos.password != null)
            ClientAuth.getInstance().setPassword(config.sos.password.toCharArray());
    }


    protected String getSosEndpointUrl()
    {
        return sosEndpointUrl;
    }
    
    
    @Override
    public void setConfiguration(SOSTClientConfig config)
    {
        super.setConfiguration(config);
         
        // compute full host URL
        String scheme = "http";
        if (config.sos.enableTLS)
            scheme = "https";
        sosEndpointUrl = scheme + "://" + config.sos.remoteHost + ":" + config.sos.remotePort;
        if (config.sos.resourcePath != null)
        {
            if (config.sos.resourcePath.charAt(0) != '/')
                sosEndpointUrl += '/';
            sosEndpointUrl += config.sos.resourcePath;
        }
    };
    
    
    protected void checkConfiguration() throws SensorHubException
    {
        // TODO check config
    }
    
    
    @Override
    public void init() throws SensorHubException
    {
        // check configuration
        checkConfiguration();
        
        // create database view using filter provided in configuration
        this.dataBaseView = config.dataSourceSelector.getFilteredView(getParentHub());
        
        // create connection handler
        this.connection = new RobustIPConnection(this, config.connection, "SOS server")
        {
            public boolean tryConnect() throws IOException
            {
                // first check if we can reach remote host on specified port
                if (!tryConnectTCP(config.sos.remoteHost, config.sos.remotePort))
                    return false;
                
                // check connection to SOS by fetching capabilities
                SOSServiceCapabilities caps = null;
                try
                {
                    GetCapabilitiesRequest request = new GetCapabilitiesRequest();
                    request.setConnectTimeOut(connectConfig.connectTimeout);
                    request.setService(SOSUtils.SOS);
                    request.setGetServer(getSosEndpointUrl());
                    caps = sosUtils.sendRequest(request, false);
                }
                catch (OWSException e)
                {
                    reportError("Cannot fetch SOS capabilities", e, true);
                    return false;
                }
                
                // check insert operations are supported
                if (!caps.getPostServers().isEmpty())
                {
                    String[] neededOps = new String[] {"InsertSensor", "InsertResultTemplate", "InsertResult"};
                    for (String opName: neededOps)
                    {
                        if (!caps.getPostServers().containsKey(opName))
                            throw new IOException(opName + " operation not supported by this SOS endpoint");
                    }
                }
                
                // check SML2 is supported
                SOSInsertionCapabilities insertCaps = caps.getInsertionCapabilities();
                if (insertCaps != null)
                {
                    if (!insertCaps.getProcedureFormats().contains(SWESUtils.DEFAULT_PROCEDURE_FORMAT))
                        throw new IOException("SensorML v2.0 format not supported by this SOS endpoint");
                    
                    if (!insertCaps.getObservationTypes().contains(IObservation.OBS_TYPE_RECORD))
                        throw new IOException("DataRecord observation type not supported by this SOS endpoint");
                }
                
                return true;
            }
        };
    }
    
    
    @Override
    public void start() throws SensorHubException
    {
        connection.updateConfig(config.connection);
        
        connection.waitForConnectionAsync()
            .thenRun(() -> {
                reportStatus("Connected to SOS endpoint " + getSosEndpointUrl());
                
                // subscribe to get notified when new procedures are added
                subscribeToRegistryEvents();
                
                // register all selected procedures and their datastreams              
                dataBaseView.getProcedureStore().selectEntries(new ProcedureFilter.Builder().build())
                    //.parallel()
                    .forEach(entry -> {
                        var id = entry.getKey().getInternalID();
                        var proc = entry.getValue();
                        try { addProcedure(id, proc); }
                        catch (Exception e) { throw new StreamException(e); }
                    });
            })
            .exceptionally(err -> {
                if (err != null)
                    reportError(err.getMessage(), err.getCause());
                return null;
            })
            .thenRun(() -> {                
                setState(ModuleState.STARTED);
            });
    }
    
    
    @Override
    public void stop() throws SensorHubException
    {
        // cancel reconnection loop
        if (connection != null)
            connection.cancel();
        
        // stop all streams
        for (var streamInfo: dataStreams.values())
            stopStream(streamInfo);
        
        // unsubscribe from procedure events
        if (registrySub != null)
            registrySub.cancel();
        for (var procInfo: procedures.values())
        {
            if (procInfo.sub != null)
                procInfo.sub.cancel();
        }
        
        // shutdown thread pool
        // will not do anything if common pool was used
        try
        {
            if (threadPool != null && !threadPool.isShutdown())
            {
                threadPool.shutdownNow();
                threadPool.awaitTermination(3, TimeUnit.SECONDS);
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
    
    
    /*
     * Subscribe to procedure registry events to get notified when
     * procedures are added or removed
     */
    protected void subscribeToRegistryEvents()
    {
        getParentHub().getEventBus().newSubscription(ProcedureEvent.class)
            .withTopicID(IProcedureRegistry.EVENT_SOURCE_ID)
            .withEventType(ProcedureEvent.class)
            .subscribe(e -> {
                handleEvent(e);
            })
            .thenAccept(sub -> {
                registrySub = sub;
                sub.request(Long.MAX_VALUE);
            });
    }
    
    
    /*
     * Register procedure, subscribe to its events and start all selected datastreams
     */
    protected void addProcedure(long procID, IProcedureWithDesc proc)
    {
        try
        {
            var procUID = proc.getUniqueIdentifier();
            var procInfo = registerSensor(procID, proc);
            
            // subscribe to procedure events
            getParentHub().getEventBus().newSubscription(ProcedureEvent.class)
                .withTopicID(EventUtils.getProcedureSourceID(procUID))
                .withEventType(ProcedureEvent.class)
                .subscribe(e -> {
                    handleEvent(e);
                })
                .thenAccept(sub -> {
                    procInfo.sub = sub;
                    sub.request(Long.MAX_VALUE);
                });
            
            // register all selected datastreams
            var dsFilter = new DataStreamFilter.Builder()
                .withProcedures(procID)
                .withCurrentVersion()
                .build();
            
            var addedStreams = new ArrayList<StreamInfo>();
            dataBaseView.getDataStreamStore().selectEntries(dsFilter)
               .forEach(e -> {
                   var dsId = e.getKey().getInternalID();
                   var dsInfo = e.getValue();
                   try
                   {
                       var streamInfo = registerDataStream(dsId, dsInfo);
                       addedStreams.add(streamInfo);
                   }
                   catch (Exception ex)
                   {
                       throw new StreamException(ex);
                   }
               });
            
            // start all streams
            for (var streamInfo: addedStreams)
            {
                try { startStream(streamInfo); }
                catch (Exception e) { throw new StreamException(e); }
            }
        }
        catch (ClientException e)
        {
            reportError(e.getMessage(), e.getCause());
        }
    }
    
    
    /*
     * Helper to register new sensor when a ProcedureAdded event is received
     */
    protected void addProcedure(String procUID)
    {
        var procEntry = dataBaseView.getProcedureStore().getCurrentVersionEntry(procUID);
        if (procEntry != null)
        {
            var id = procEntry.getKey().getInternalID();
            var proc = procEntry.getValue();
            addProcedure(id, proc);
        }
    }
    
    
    /*
     * Register sensor at remote SOS
     */
    protected ProcedureInfo registerSensor(long procID, IProcedureWithDesc proc) throws ClientException
    {
        var procUID = proc.getUniqueIdentifier();
        
        // skip if already registered
        var procInfo = procedures.get(procUID);
        if (procInfo != null)
        {
            // if description is newer, call updateSensor
            if (proc.getValidTime().begin().isAfter(procInfo.startValidTime))
                updateSensor(procUID);
            return procInfo;
        }
        
        try
        {
            // build insert sensor request
            InsertSensorRequest req = new InsertSensorRequest();
            req.setConnectTimeOut(config.connection.connectTimeout);
            req.setPostServer(getSosEndpointUrl());
            req.setVersion("2.0");
            req.setProcedureDescription(proc.getFullDescription());
            req.setProcedureDescriptionFormat(SWESUtils.DEFAULT_PROCEDURE_FORMAT);
            req.getObservationTypes().add(IObservation.OBS_TYPE_RECORD);
            req.getFoiTypes().add("gml:Feature");
            
            // send request and get assigned ID
            setAuth();
            InsertSensorResponse resp = sosUtils.sendRequest(req, false);

            // add procedure info to map
            procInfo = new ProcedureInfo();
            procInfo.offeringId = resp.getAssignedOffering();
            procInfo.startValidTime = proc.getValidTime().begin();
            procedures.put(procUID, procInfo);
            
            getLogger().info("Procedure {} registered at SOS endpoint {}", procUID, getSosEndpointUrl());
            return procInfo;
        }
        catch (Exception e)
        {
            throw new ClientException("Error registering procedure " + procUID + " at remote SOS", e);
        }
    }
    
    
    /*
     * Update sensor description at remote SOS
     */
    protected void updateSensor(String procUID)
    {
        try
        {
            var proc = dataBaseView.getProcedureStore().getCurrentVersion(procUID);
            if (proc != null)
            {
                // build update sensor request
                UpdateSensorRequest req = new UpdateSensorRequest(SOSUtils.SOS);
                req.setConnectTimeOut(config.connection.connectTimeout);
                req.setPostServer(getSosEndpointUrl());
                req.setVersion("2.0");
                req.setProcedureId(proc.getUniqueIdentifier());
                req.setProcedureDescription(proc.getFullDescription());
                req.setProcedureDescriptionFormat(SWESUtils.DEFAULT_PROCEDURE_FORMAT);
                
                // send request
                setAuth();
                sosUtils.sendRequest(req, false);
                
                getLogger().info("Procedure {} updated at SOS endpoint {}", procUID, getSosEndpointUrl());
            }
        }
        catch (Exception e)
        {
            reportError("Error updating procedure " + procUID + " at remote SOS", e);
        }
    }
    
    
    /*
     * Disable and optionally remove previously managed procedure
     */
    protected void disableProcedure(String procUID, boolean remove)
    {
        var procInfo = remove ? procedures.remove(procUID) : procedures.get(procUID);
        if (procInfo != null)
        {
            if (procInfo.sub != null)
            {
                procInfo.sub.cancel();
                procInfo.sub = null;
                getLogger().debug("Unsubscribed from procedure {}", procUID);
            }
            
            var procDataStreams = dataStreams.subMap(procUID, procUID + "\uffff");
            for (var streamInfo: procDataStreams.values())
                stopStream(streamInfo);
            
            if (remove)
                getLogger().info("Removed procedure {}", procUID);
        }
    }
    
    
    /*
     * Register datastream at remote SOS
     */
    protected StreamInfo registerDataStream(long dsID, IDataStreamInfo dsInfo) throws ClientException
    {
        try
        {
            var dsSourceId = getDataStreamSourceId(dsInfo);
            
            // skip if already registered
            var streamInfo = dataStreams.get(dsSourceId);
            if (streamInfo != null)
                return streamInfo;
            
            // retrieve procedure info
            var procInfo = procedures.get(dsInfo.getProcedureID().getUniqueID());
            if (procInfo == null)
                throw new IllegalStateException("Unknown procedure: " + dsInfo.getProcedureID().getUniqueID());
                        
            // otherwise register result template
            // create request object
            InsertResultTemplateRequest req = new InsertResultTemplateRequest();
            req.setConnectTimeOut(config.connection.connectTimeout);
            req.setPostServer(getSosEndpointUrl());
            req.setVersion("2.0");
            req.setOffering(procInfo.offeringId);
            req.setResultStructure(dsInfo.getRecordStructure());
            req.setResultEncoding(dsInfo.getRecordEncoding());
            ObservationImpl obsTemplate = new ObservationImpl();
            req.setObservationTemplate(obsTemplate);
            
            // set FOI if known
            dataBaseView.getObservationStore().selectObservedFois(
                new ObsFilter.Builder()
                    .withDataStreams(dsID)
                    .withPhenomenonTime().withCurrentTime().done()
                    .build())
                .findFirst().ifPresent(id -> {
                    IGeoFeature foi = dataBaseView.getFoiStore().getCurrentVersion(id);
                    if (foi != null)
                        obsTemplate.setFeatureOfInterest(foi);
                });
            
            // send request
            setAuth();
            InsertResultTemplateResponse resp = sosUtils.sendRequest(req, false);
            
            // add stream info to map
            streamInfo = new StreamInfo();
            streamInfo.internalID = dsID;
            streamInfo.procUID = dsInfo.getProcedureID().getUniqueID();
            streamInfo.outputName = dsInfo.getOutputName();
            streamInfo.eventSrcId = dsSourceId;
            streamInfo.templateId = resp.getAcceptedTemplateId();
            streamInfo.resultData.setElementType(dsInfo.getRecordStructure());
            streamInfo.resultData.setEncoding(dsInfo.getRecordEncoding());
            //streamInfo.measPeriodMs = (int)(output.getAverageSamplingPeriod()*1000);
            streamInfo.minRecordsPerRequest = 1;//(int)(1.0 / sensorOutput.getAverageSamplingPeriod());
            dataStreams.put(streamInfo.eventSrcId, streamInfo);
            
            getLogger().info("Datastream {} registered at SOS endpoint {}", 
                getDataStreamSourceId(dsInfo), getSosEndpointUrl());
                        
            return streamInfo;
        }
        catch (Exception e)
        {
            throw new ClientException("Error registering datastream " + 
                getDataStreamSourceId(dsInfo) + " at remote SOS", e);
        }
    }
    
    
    /*
     * Subscribe to eventbus and start sending data data to remote SOS
     */
    protected void startStream(StreamInfo streamInfo) throws ClientException
    {
        try
        {
            // skip if stream was already started
            if (streamInfo.sub != null)
                return;
            
            // subscribe to data events
            getParentHub().getEventBus().newSubscription(DataEvent.class)
                .withTopicID(streamInfo.eventSrcId)
                .withEventType(DataEvent.class)
                .subscribe(e -> {
                    handleEvent(e, streamInfo);
                })
                .thenAccept(sub -> {
                    streamInfo.sub = sub;
                    sub.request(Long.MAX_VALUE);
                    
                    // send latest record(s)
                    dataBaseView.getObservationStore().select(new ObsFilter.Builder()
                            .withDataStreams(streamInfo.internalID)
                            .withLatestResult()
                            .build())
                        .forEach(obs -> {
                            sendObs(obs, streamInfo);
                        });
                    
                    getLogger().info("Starting data push for stream {} to SOS endpoint {}", streamInfo.eventSrcId, getSosEndpointUrl());
                });
        }
        catch(Exception e)
        {
            throw new ClientException("Error starting data push for stream " + streamInfo.eventSrcId, e);
        }
    }
    
    
    /*
     * Helper to register and start new datastream when a DataStreamAdded event is received
     */
    protected void addAndStartStream(String procUID, String outputName)
    {
        try
        {
            var dsEntry = dataBaseView.getDataStreamStore().getLatestVersionEntry(procUID, outputName);
            if (dsEntry != null)
            {
                var dsId = dsEntry.getKey().getInternalID();
                var dsInfo = dsEntry.getValue();
                
                var streamInfo = registerDataStream(dsId, dsInfo);
                startStream(streamInfo);
            }
        }
        catch (ClientException e)
        {
            reportError(e.getMessage(), e.getCause());
        }
    }
    
    
    /*
     * Disable and optionally remove previously managed datastream
     */
    protected void disableDataStream(String procUID, String outputName, boolean remove)
    {
        var dsSourceId = getDataStreamSourceId(procUID, outputName);
        var streamInfo = remove ? dataStreams.remove(dsSourceId) : dataStreams.get(dsSourceId);
        if (streamInfo != null)
        {
            stopStream(streamInfo);
            if (remove)
                getLogger().info("Removed datastream {}", dsSourceId);
        }
    }
    
    
    /*
     * Stop listening and pushing data for the given stream
     */
    protected void stopStream(StreamInfo streamInfo)
    {
        // unsubscribe from eventbus
        if (streamInfo.sub != null)
        {
            streamInfo.sub.cancel();
            streamInfo.sub = null;
        }
        
        // close persistent HTTP connection
        try
        {
            streamInfo.stopping = true;
            if (streamInfo.persistentWriter != null)
                streamInfo.persistentWriter.close();
            if (streamInfo.connection != null)
                streamInfo.connection.disconnect();
        }
        catch (IOException e)
        {
            if (getLogger().isDebugEnabled())
                getLogger().error("Cannot close persistent connection", e);
        }
        finally
        {
            streamInfo.persistentWriter = null;
            streamInfo.connection = null;
        }
        
        // clear executor queue
        if (streamInfo.executor != null)
            streamInfo.executor.clear();
        
        getLogger().info("Stopping data push for stream {}", streamInfo.eventSrcId);
    }
    
    
    protected void handleEvent(final DataEvent e, final StreamInfo streamInfo)
    {
        // we stop here if we had too many errors
        if (streamInfo.errorCount >= config.connection.maxConnectErrors)
        {
            String outputName = ((DataEvent)e).getSource().getName();
            reportError("Too many errors sending '" + outputName + "' data to SOS-T. Stopping Stream.", null);
            stopStream(streamInfo);
            checkDisconnected();                
            return;
        }
        
        // skip if we cannot handle more requests
        if (streamInfo.executor.getQueue().size() == config.connection.maxQueueSize)
        {
            String outputName = ((DataEvent)e).getSource().getName();
            getLogger().warn("Too many '{}' records to send to SOS-T. Bandwidth cannot keep up.", outputName);
            getLogger().info("Skipping records by purging record queue");
            streamInfo.executor.clear();
            return;
        }
        
        // record last event time
        streamInfo.lastEventTime = e.getTimeStamp();
        
        // send obs to remote SOS
        sendObs(e, streamInfo);
    }
    
    
    protected void handleEvent(final ProcedureEvent e)
    {
        // sensor description updated
        if (e instanceof ProcedureChangedEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                updateSensor(procUID);
            });
        }
        
        // procedure events
        else if (e instanceof ProcedureAddedEvent || e instanceof ProcedureEnabledEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                addProcedure(procUID);
            });
        }
        
        else if (e instanceof ProcedureDisabledEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                disableProcedure(procUID, false);
            });
        }
        
        else if (e instanceof ProcedureRemovedEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                disableProcedure(procUID, true);
            });
        }
        
        // datastream events
        else if (e instanceof DataStreamAddedEvent || e instanceof DataStreamEnabledEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                var outputName = ((DataStreamAddedEvent) e).getOutputName();
                addAndStartStream(procUID, outputName);
            });
        }
        
        else if (e instanceof DataStreamDisabledEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                var outputName = ((DataStreamAddedEvent) e).getOutputName();
                disableDataStream(procUID, outputName, false);
            });
        }
        
        else if (e instanceof DataStreamRemovedEvent)
        {
            CompletableFuture.runAsync(() -> {
                var procUID = e.getProcedureUID();
                var outputName = ((DataStreamAddedEvent) e).getOutputName();
                disableDataStream(procUID, outputName, true);
            });
        }
    }
    
    
    private void checkDisconnected()
    {
        // if all streams have been stopped, initiate reconnection
        boolean allStopped = true;
        for (StreamInfo streamInfo: dataStreams.values())
        {
            if (!streamInfo.stopping)
            {
                allStopped = false;
                break;
            }
        }
        
        if (allStopped)
        {
            reportStatus("All streams stopped on error. Trying to reconnect...");
            connection.reconnect();
        }
    }
    
    
    private void sendObs(final IObsData obs, final StreamInfo streamInfo)
    {
        sendObs(new DataEvent(
            obs.getResultTime().toEpochMilli(),
            streamInfo.procUID,
            streamInfo.outputName,
            obs.getFoiID().getUniqueID(),
            obs.getResult()), streamInfo);
    }
    
    
    private void sendObs(final DataEvent e, final StreamInfo streamInfo)
    {
        // send record using one of 2 methods
        if (config.connection.usePersistentConnection)
            sendInPersistentRequest((DataEvent)e, streamInfo);
        else
            sendAsNewRequest((DataEvent)e, streamInfo);
    }
    
    
    /*
     * Sends each new record using an XML InsertResult POST request
     */
    private void sendAsNewRequest(final DataEvent e, final StreamInfo streamInfo)
    {
        // append records to buffer
        for (DataBlock record: e.getRecords())
            streamInfo.resultData.pushNextDataBlock(record);
        
        // send request if min record count is reached
        if (streamInfo.resultData.getNumElements() >= streamInfo.minRecordsPerRequest)
        {
            final InsertResultRequest req = new InsertResultRequest();
            req.setPostServer(getSosEndpointUrl());
            req.setVersion("2.0");
            req.setTemplateId(streamInfo.templateId);
            req.setResultData(streamInfo.resultData);
            
            // create new container for future data
            streamInfo.resultData = streamInfo.resultData.copy();
            
            // create send request task
            Runnable sendTask = new Runnable() {
                @Override
                public void run()
                {
                    try
                    {
                        if (getLogger().isTraceEnabled())
                        {
                            String outputName = e.getSource().getName();
                            int numRecords = req.getResultData().getComponentCount();
                            getLogger().trace("Sending " + numRecords + " '" + outputName + "' record(s) to SOS-T");
                            getLogger().trace("Queue size is " + streamInfo.executor.getQueue().size());
                        }
                        
                        sosUtils.sendRequest(req, false);
                    }
                    catch (Exception ex)
                    {
                        String outputName = e.getSource().getName();
                        reportError("Error when sending '" + outputName + "' data to SOS-T", ex, true);
                        streamInfo.errorCount++;
                    }
                }           
            };
            
            // run task in async thread pool
            streamInfo.executor.execute(sendTask);
        }
    }
    
    
    /*
     * Sends all records in the same persistent HTTP connection.
     * The connection is created when the first record is received
     */
    private void sendInPersistentRequest(final DataEvent e, final StreamInfo streamInfo)
    {
        // skip records while we are connecting to remote SOS
        if (streamInfo.connecting)
            return;
        
        // create send request task
        Runnable sendTask = new Runnable() {
            @Override
            public void run()
            {
                try
                {
                    // connect if not already connected
                    if (streamInfo.persistentWriter == null)
                    {                        
                        streamInfo.connecting = true;
                        if (getLogger().isDebugEnabled())
                            getLogger().debug("Initiating persistent HTTP request");
                        
                        final InsertResultRequest req = new InsertResultRequest();
                        req.setPostServer(getSosEndpointUrl());
                        req.setVersion("2.0");
                        req.setTemplateId(streamInfo.templateId);
                        
                        // connect to server                        
                        HttpURLConnection conn = sosUtils.sendPostRequestWithQuery(req);                        
                        conn.setRequestProperty("Content-type", "text/plain");
                        conn.setChunkedStreamingMode(32);
                        if (config.sos.user != null && config.sos.password != null)
                        {
                            // need to configure auth manually when using streaming
                            byte[] data = (config.sos.user+":"+config.sos.password).getBytes("UTF-8");
                            String encoded = new String(Base64Encoder.encode(data));
                            conn.setRequestProperty("Authorization", "Basic "+encoded);
                        }
                        conn.connect();
                        streamInfo.connection = conn;
                        
                        // prepare writer
                        streamInfo.persistentWriter = streamInfo.resultData.getDataWriter();
                        streamInfo.persistentWriter.setOutput(new BufferedOutputStream(conn.getOutputStream()));
                        streamInfo.connecting = false;
                    }
                    
                    if (getLogger().isTraceEnabled())
                    {
                        String outputName = e.getSource().getName();
                        int numRecords = e.getRecords().length;
                        getLogger().trace("Sending " + numRecords + " '" + outputName + "' record(s) to SOS-T");
                        getLogger().trace("Queue size is " + streamInfo.executor.getQueue().size());
                    }
                    
                    // write records to output stream
                    for (DataBlock record: e.getRecords())
                        streamInfo.persistentWriter.write(record);
                    streamInfo.persistentWriter.flush();
                }
                catch (Exception ex)
                {
                    // ignore exception if stream was purposely stopped
                    if (streamInfo.stopping)
                        return;
                    
                    String outputName = e.getSource().getName();
                    reportError("Error when sending '" + outputName + "' data to SOS-T", ex, true);
                    streamInfo.errorCount++;
                    
                    try
                    {
                        if (streamInfo.persistentWriter != null)
                            streamInfo.persistentWriter.close();
                    }
                    catch (IOException e1)
                    {
                        getLogger().trace("Cannot close persistent connection", e1);
                    }
                    
                    // clean writer so we reconnect
                    streamInfo.persistentWriter = null;
                    streamInfo.connecting = false;
                }
            }           
        };
        
        // run task in async thread pool
        streamInfo.executor.execute(sendTask);
    }
    
    
    private String getDataStreamSourceId(IDataStreamInfo dsInfo)
    {
        return getDataStreamSourceId(
            dsInfo.getProcedureID().getUniqueID(),
            dsInfo.getOutputName());
    }
    
    
    private String getDataStreamSourceId(String procUID, String outputName)
    {
        return EventUtils.getProcedureOutputSourceID(procUID, outputName);
    }


    @Override
    public boolean isConnected()
    {
        return connection.isConnected();
    }
    
    
    public Map<String, StreamInfo> getDataStreams()
    {
        return dataStreams;
    }


    @Override
    public void cleanup() throws SensorHubException
    {
        // nothing to clean
    }
}
