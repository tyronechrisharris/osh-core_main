/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Subscription;
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IMultiSourceDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.datastore.DataStreamInfo;
import org.sensorhub.api.datastore.FeatureId;
import org.sensorhub.api.datastore.FeatureKey;
import org.sensorhub.api.datastore.IDataStreamStore;
import org.sensorhub.api.datastore.IHistoricalObsAutoPurgePolicy;
import org.sensorhub.api.datastore.IHistoricalObsDatabase;
import org.sensorhub.api.datastore.ObsData;
import org.sensorhub.api.datastore.ObsKey;
import org.sensorhub.api.event.ISubscriptionBuilder;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.persistence.StorageConfig;
import org.sensorhub.api.persistence.StorageException;
import org.sensorhub.api.procedure.IProcedureRegistry;
import org.sensorhub.api.procedure.IProcedureWithState;
import org.sensorhub.api.procedure.ProcedureAddedEvent;
import org.sensorhub.api.procedure.ProcedureChangedEvent;
import org.sensorhub.api.procedure.ProcedureDisabledEvent;
import org.sensorhub.api.procedure.ProcedureEnabledEvent;
import org.sensorhub.api.procedure.ProcedureEvent;
import org.sensorhub.api.procedure.ProcedureRemovedEvent;
import org.sensorhub.impl.module.AbstractModule;
import org.sensorhub.utils.MsgUtils;
import org.sensorhub.utils.SWEDataUtils;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;


/**
 * <p>
 * Generic wrapper/adapter enabling any {@link IHistoricalObsDatabase}
 * implementation to store data coming from data events (e.g. sensor data,
 * processed data, etc.).
 * </p><p>
 * This class takes care of registering with the appropriate producers and
 * uses the data store API to store records in the underlying database.
 * </p>
 *
 * @author Alex Robin
 * @since Sep 23, 2019
 */
public class GenericObsStreamDataStore extends AbstractModule<StreamDataStoreConfig>
{
    static final String WAITING_STATUS_MSG = "Waiting for data source {}";
    
    IHistoricalObsDatabase db;
    Map<String, ProducerInfo> registeredProducers = new ConcurrentHashMap<>(); // key = procedure UID
    long lastCommitTime = Long.MIN_VALUE;
    Timer autoPurgeTimer;
    Subscription procRegistrySub;
    boolean processEvents;
    
    
    static class ProducerInfo
    {
        Map<String, DataStreamCachedInfo> dataStreams = new HashMap<>();
        Subscription eventSub;
        FeatureId currentFoi = ObsKey.NO_FOI;
    }
    
    
    static class DataStreamCachedInfo
    {
        long dataStreamID;
        ScalarIndexer timeStampIndexer;
    }
    
    
    @Override
    public void start() throws SensorHubException
    {
        if (config.dbConfig == null)
            throw new StorageException("Underlying storage configuration must be provided");
        
        processEvents = config.processEvents;
        
        // instantiate and start underlying storage
        StorageConfig storageConfig = null;
        try
        {
            storageConfig = (StorageConfig)config.dbConfig.clone();
            storageConfig.id = getLocalID();
            storageConfig.name = getName();
            Class<?> clazz = Class.forName(storageConfig.moduleClass);
            
            @SuppressWarnings("unchecked")
            IModule<StorageConfig> dbModule = (IModule<StorageConfig>)clazz.getDeclaredConstructor().newInstance();
            dbModule.setParentHub(getParentHub());
            dbModule.init(storageConfig);
            dbModule.start();
            
            this.db = (IHistoricalObsDatabase)dbModule;
            Asserts.checkNotNull(db.getProcedureStore());
            Asserts.checkNotNull(db.getFoiStore());
            Asserts.checkNotNull(db.getObservationStore());
        }
        catch (Exception e)
        {
            throw new StorageException("Cannot instantiate underlying storage " + storageConfig.moduleClass, e);
        }
        
        // start auto-purge timer thread if policy is specified and enabled
        if (config.autoPurgeConfig != null && config.autoPurgeConfig.enabled)
        {
            final IHistoricalObsAutoPurgePolicy policy = config.autoPurgeConfig.getPolicy();
            autoPurgeTimer = new Timer();
            TimerTask task = new TimerTask() {
                public void run()
                {
                    policy.trimStorage(db, logger);
                }
            };
            
            autoPurgeTimer.schedule(task, 0, (long)(config.autoPurgeConfig.purgePeriod*1000));
        }
        
        // make sure we get notified if procedures are added later
        subscribeToProcedureRegistryEvents()
            .thenAccept(s -> {
                
                // keep handle to subscription so we can cancel it later
                procRegistrySub = s;
        
                // load data sources that are already enabled
                for (String id: config.procedureUIDs)
                {
                    IDataProducer dataSource = getDataSource(id);
                    if (dataSource != null)
                        addProcedure(dataSource);
                }
            });
    }
    
    
    protected IDataProducer getDataSource(String procOrModuleID)
    {
        IProcedureWithState proc = getParentHub().getProcedureRegistry().get(procOrModuleID);
        if (proc == null || !(proc instanceof IDataProducer))
            return null;
        return (IDataProducer)proc;
    }
    
    
    protected synchronized void addProcedure(IDataProducer dataSource)
    {
        String uid = dataSource.getUniqueIdentifier();
        if (dataSource.isEnabled() && !registeredProducers.containsKey(uid))
        {
            connectDataSource(dataSource);
            getLogger().info("Connected to data source " + MsgUtils.entityString(dataSource));
            getParentHub().getDatabaseRegistry().register(Arrays.asList(uid), db);
        }
    }
    
    
    protected synchronized void removeProcedure(IDataProducer dataSource)
    {
        disconnectDataSource(dataSource);
        getLogger().info("Disconnected from data source " + MsgUtils.entityString(dataSource));
    }
    
    
    /*
     * Add new procedure if it is associated to this database
     */
    protected void maybeAddProcedure(IDataProducer dataSource)
    {
        String uid = dataSource.getUniqueIdentifier();
        String parentUid = dataSource.getParentGroupUID();
        
        // check if UID or parent UID was configured
        if (config.procedureUIDs.contains(uid) || config.procedureUIDs.contains(parentUid))
            addProcedure(dataSource);
    }
    
    
    protected CompletableFuture<Subscription> subscribeToProcedureRegistryEvents()
    {     
        return getParentHub().getEventBus().newSubscription(ProcedureEvent.class)
            .withSourceID(IProcedureRegistry.EVENT_SOURCE_ID)
            .withEventType(ProcedureAddedEvent.class)
            .withEventType(ProcedureRemovedEvent.class)
            .withEventType(ProcedureEnabledEvent.class)
            .withEventType(ProcedureDisabledEvent.class)
            .consume(e -> {
                String procID = e.getProcedureUID();
                IDataProducer dataSource = getDataSource(procID);
                if (e instanceof ProcedureAddedEvent || e instanceof ProcedureEnabledEvent)
                    maybeAddProcedure(dataSource);
                else
                    removeProcedure(dataSource);
            });
    }
    
    
    /*
     * Gets the list of selected outputs (i.e. a subset of all data source outputs)
     */
    protected Collection<? extends IStreamingDataInterface> getSelectedOutputs(IDataProducer dataSource)
    {
        return dataSource.getOutputs().values();
        
        /*if (config.excludedOutputs == null || config.excludedOutputs.isEmpty())
        {
            return dataSource.getOutputs().values();
        }
        else
        {
            List <IStreamingDataInterface> selectedOutputs = new ArrayList<>();
            for (IStreamingDataInterface outputInterface : dataSource.getOutputs().values())
            {
                // skip excluded outputs
                if (!config.excludedOutputs.contains(outputInterface.getName()))
                    selectedOutputs.add(outputInterface);
            }
            
            return selectedOutputs;
        }*/
    }
    
    
    /*
     * Connects to data source and store initial metadata for all selected streams
     */
    protected void connectDataSource(final IDataProducer dataSource)
    {
        Asserts.checkNotNull(dataSource, IDataProducer.class);
        Asserts.checkNotNull(dataSource.getUniqueIdentifier(), "uniqueID");
        
        String procUID = dataSource.getUniqueIdentifier();
        ProducerInfo producerInfo = registeredProducers.computeIfAbsent(procUID, k -> new ProducerInfo());
        FeatureKey procKey = db.getProcedureStore().getLatestVersionKey(procUID);
        
        // need to make sure we add things if they are missing in storage
        
        // store data source description if none was found
        if (procKey == null)
        {
            procKey = db.getProcedureStore().add(dataSource.getCurrentDescription());
        }
        
        // otherwise update to the latest sensor description in case we were down during the last update
        else if (dataSource.getLastDescriptionUpdate() != Long.MIN_VALUE)
        {
            Instant validStartTime = dataSource.getCurrentDescription().getValidTime().lowerEndpoint();
            if (procKey.getValidStartTime().isBefore(validStartTime))
                procKey = db.getProcedureStore().addVersion(dataSource.getCurrentDescription());
        }
        
        // add data stream for each selected output that is not already registered
        IDataStreamStore dataStreamStore = db.getObservationStore().getDataStreams();
        var selectedOutputs = getSelectedOutputs(dataSource);
        for (IStreamingDataInterface output: selectedOutputs)
        {
            // try to retrieve existing data stream
            Entry<Long, DataStreamInfo> dsEntry = dataStreamStore.getLastEntry(procUID, output.getName());
            Long dsID;
            
            if (dsEntry == null)
            {
                // create new data stream
                DataStreamInfo dsInfo = DataStreamInfo.builder()
                    .withProcedure(procKey)
                    .withRecordDescription(output.getRecordDescription())
                    .withRecordEncoding(output.getRecommendedEncoding())
                    .build();
                dsID = dataStreamStore.add(dsInfo);
            }
            else
            {
                dsID = dsEntry.getKey();
                DataStreamInfo dsInfo = dsEntry.getValue();
                
                if (hasOutputChanged(dsInfo.getRecordDescription(), output.getRecordDescription()))
                {
                    // version existing data stream
                    dsInfo = DataStreamInfo.builder()
                        .withProcedure(procKey)
                        .withRecordDescription(output.getRecordDescription())
                        .withRecordEncoding(output.getRecommendedEncoding())
                        .withRecordVersion(dsInfo.getRecordVersion()+1)
                        .build();
                    dsID = dataStreamStore.add(dsInfo);
                }
            }
            
            // cache info about this data stream
            DataStreamCachedInfo cachedInfo = new DataStreamCachedInfo();
            cachedInfo.dataStreamID = dsID;
            cachedInfo.timeStampIndexer = SWEHelper.getTimeStampIndexer(output.getRecordDescription());
            producerInfo.dataStreams.put(output.getName(), cachedInfo);
        }
        
        // init current FOI
        AbstractFeature foi = dataSource.getCurrentFeatureOfInterest();
        if (foi != null)
        {
            // TODO support versioning but check that fois are really different            
            FeatureKey fk = db.getFoiStore().getLatestVersionKey(foi.getUniqueIdentifier());
            if (fk == null)
                fk = db.getFoiStore().addVersion(foi);
            producerInfo.currentFoi = fk;
        }
        
        // make sure changes are written to storage
        db.commit();
        
        // register to procedure and output data events
        if (producerInfo.eventSub == null)
        {
            subscribeToProcedureEvents(dataSource, selectedOutputs)
                .thenAccept(s -> {
                    getLogger().debug("Subscribed to output data events from {}", MsgUtils.entityString(dataSource));
                    producerInfo.eventSub = s;
                    s.request(Long.MAX_VALUE); // no back pressure
                });
        }
        
        // if multisource, call recursively to connect nested producers
        if (dataSource instanceof IMultiSourceDataProducer)
        {
            IMultiSourceDataProducer multiSource = (IMultiSourceDataProducer)dataSource;
            for (String nestedProducerID: multiSource.getMembers().keySet())
                ensureProducerInfo(nestedProducerID);
        }       
    }
    
    
    protected boolean hasOutputChanged(DataComponent liveVersion, DataComponent storedVersion)
    {
        // TODO detect if data stream versioning is needed
        return false;
    }
    
    
    /*
     * Ensure producer metadata has been initialized
     */
    protected void ensureProducerInfo(String producerID)    
    {
        IDataProducer dataSource = getDataSource(producerID);
        if (dataSource != null)
            connectDataSource(dataSource);
    }
    
    
    /*
     * Disconnect from data source
     */
    protected void disconnectDataSource(IDataProducer dataSource)
    {
        Asserts.checkNotNull(dataSource, IDataProducer.class);
        
        // if multisource, disconnects from all nested producers
        if (dataSource instanceof IMultiSourceDataProducer)
        {
            IMultiSourceDataProducer multiSource = (IMultiSourceDataProducer)dataSource;
            for (String entityID: multiSource.getMembers().keySet())
                disconnectDataSource(multiSource.getMembers().get(entityID));
        }
        
        // remove producer and cancel subscriptions
        ProducerInfo procInfo = registeredProducers.remove(dataSource.getUniqueIdentifier());
        if (procInfo != null)
            procInfo.eventSub.cancel();
    }
    
    
    /*
     * Subscribe to data, foi and procedure changed events coming from the
     * specified producer
     */
    protected CompletableFuture<Subscription> subscribeToProcedureEvents(IDataProducer producer, Collection<? extends IStreamingDataInterface> selectedOutputs)
    {     
        // get subscription builder
        ISubscriptionBuilder<ProcedureEvent> subscription = getParentHub().getEventBus()
            .newSubscription(ProcedureEvent.class)
            .withEventType(DataEvent.class)
            .withEventType(FoiEvent.class)
            .withEventType(ProcedureChangedEvent.class)
            .withSource(producer); // to get procedure changed events
        
        // add selected outputs to subscription
        for (IStreamingDataInterface output: selectedOutputs)
        {
            // fetch latest record
            DataBlock rec = output.getLatestRecord();
            if (rec != null)
                handleEvent(new DataEvent(System.currentTimeMillis(), output, rec));
        
            // add output to subscription
            subscription.withSource(output);
        }
        
        // finalize subscription
        return subscription.subscribe(
            this::handleEvent,
            err -> {
                getLogger().error("Error during event dispatch", err);
            },
            () -> {});
    }
    
        
    @Override
    public synchronized void stop() throws SensorHubException
    {
        processEvents = false;
        
        // unsubscribe from procedure registry
        if (procRegistrySub != null)
            procRegistrySub.cancel();
            
        // unsubscribe from all procedure events
        for (ProducerInfo procInfo: registeredProducers.values())
            procInfo.eventSub.cancel();
        registeredProducers.clear();
        
        if (autoPurgeTimer != null)
            autoPurgeTimer.cancel();

        if (db != null && db instanceof IModule<?>)
            ((IModule<?>)db).stop();
    }
    
    
    public void handleEvent(ProcedureEvent e)
    {
        if (processEvents)
        {
            // new data events
            if (e instanceof DataEvent)
            {
                DataEvent dataEvent = (DataEvent)e;
                String producerID = dataEvent.getProcedureUID();
                                
                if (producerID != null)
                {
                    ProducerInfo procInfo = registeredProducers.get(producerID);
                    if (procInfo == null)
                        return;
                    
                    // look up data stream cached info
                    String outputName = dataEvent.getChannelID();
                    DataStreamCachedInfo dsInfo = procInfo.dataStreams.get(outputName);
                    
                    // store all records
                    for (DataBlock record: dataEvent.getRecords())
                    {
                        // get time stamp
                        double time;
                        if (dsInfo.timeStampIndexer != null)
                            time = dsInfo.timeStampIndexer.getDoubleValue(record);
                        else
                            time = e.getTimeStamp() / 1000.;
                    
                        // store record with proper key
                        ObsKey obsKey = ObsKey.builder()
                            .withDataStream(dsInfo.dataStreamID)
                            .withFoi(procInfo.currentFoi)
                            .withPhenomenonTime(SWEDataUtils.toInstant(time))
                            .build();
                        
                        ObsData obs = ObsData.builder()
                            .withResult(record)
                            .build();
                        
                        db.getObservationStore().put(obsKey, obs);                        
                        getLogger().trace("Storing observation with key {}", obsKey);
                    }
                }
            }
            
            else if (e instanceof FoiEvent)
            {
                FoiEvent foiEvent = (FoiEvent)e;
                String producerID = foiEvent.getProcedureUID();
                
                if (producerID != null)
                {
                    ProducerInfo procInfo = registeredProducers.get(producerID);
                    if (procInfo == null)
                        return;
                    
                    // store feature object if specified
                    FeatureKey fk = db.getFoiStore().getLatestVersionKey(foiEvent.getFoiUID());
                    if (foiEvent.getFoi() != null && fk == null)
                    {
                        fk = db.getFoiStore().add(foiEvent.getFoi());
                        getLogger().trace("Storing foi {}", fk);
                    }
                
                    // also remember as current FOI
                    procInfo.currentFoi = fk;
                }
            }
            
            else if (e instanceof ProcedureChangedEvent)
            {
                // TODO check that description was actually updated?
                // in the current state, the same description would be added at each restart
                // should we compare contents? if not, on what time tag can we rely on?
                // AbstractSensorModule implementation of getLastSensorDescriptionUpdate() is
                // only useful between restarts since it will be resetted to current time at startup...
                
                // TODO to manage this issue, first check that no other description is valid at the same time
                //storage.storeDataSourceDescription(dataSourceRef.get().getCurrentDescription());
                String producerID = ((ProcedureChangedEvent)e).getProcedureUID();
                IDataProducer producer = getDataSource(producerID);
                if (producer != null)
                    db.getProcedureStore().addVersion(producer.getCurrentDescription());
            }
            
            // commit only when necessary
            long now = System.currentTimeMillis();
            if (lastCommitTime == Long.MIN_VALUE || (now - lastCommitTime) > config.minCommitPeriod)
            {
                db.commit();
                lastCommitTime = now;
            }
        }
    }
    
    
    public IHistoricalObsDatabase getDatabase()
    {
        return db;
    }
}
