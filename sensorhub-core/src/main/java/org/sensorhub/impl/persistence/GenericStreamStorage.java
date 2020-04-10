/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow.Subscription;
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.api.data.FoiEvent;
import org.sensorhub.api.data.IDataProducer;
import org.sensorhub.api.data.IMultiSourceDataProducer;
import org.sensorhub.api.data.IStreamingDataInterface;
import org.sensorhub.api.event.ISubscriptionBuilder;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.persistence.DataKey;
import org.sensorhub.api.persistence.IBasicStorage;
import org.sensorhub.api.persistence.IFoiFilter;
import org.sensorhub.api.persistence.IMultiSourceStorage;
import org.sensorhub.api.persistence.IObsStorage;
import org.sensorhub.api.persistence.IRecordStorageModule;
import org.sensorhub.api.persistence.IDataFilter;
import org.sensorhub.api.persistence.IDataRecord;
import org.sensorhub.api.persistence.IStorageModule;
import org.sensorhub.api.persistence.IRecordStoreInfo;
import org.sensorhub.api.persistence.ObsKey;
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
import org.vast.ogc.gml.IGeoFeature;
import org.vast.swe.SWEHelper;
import org.vast.swe.ScalarIndexer;
import org.vast.util.Asserts;
import org.vast.util.Bbox;


/**
 * <p>
 * Generic wrapper/adapter enabling any storage implementation to store data
 * coming from data events (e.g. sensor data, processed data, etc.)<br/>
 * This class takes care of registering with the appropriate producers and
 * uses the storage API to store records in the underlying storage.
 * </p>
 *
 * @author Alex Robin
 * @since Feb 21, 2015
 */
public class GenericStreamStorage extends AbstractModule<StreamStorageConfig> implements IRecordStorageModule<StreamStorageConfig>,
    IObsStorage, IMultiSourceStorage<IObsStorage>
{
    static final String WAITING_STATUS_MSG = "Waiting for data source ";
    
    IRecordStorageModule<StorageConfig> storage;
    WeakReference<IDataProducer> dataSourceRef;
    Map<String, ScalarIndexer> timeStampIndexers = new HashMap<>();
    Map<String, String> currentFoiMap = new ConcurrentHashMap<>(); // entity ID -> current FOI ID
    Map<String, Subscription> registeredProducers = new ConcurrentHashMap<>(); // producerId -> Subscription
    String producerUID;
    long lastCommitTime = Long.MIN_VALUE;
    Timer autoPurgeTimer;
    Subscription procRegistrySub;
    boolean processEvents;
    
    
    @Override
    public void start() throws SensorHubException
    {
        if (config.storageConfig == null)
            throw new StorageException("Underlying storage configuration must be provided");
        
        processEvents = config.processEvents;
        
        // instantiate and start underlying storage
        StorageConfig storageConfig = null;
        try
        {
            storageConfig = (StorageConfig)config.storageConfig.clone();
            storageConfig.id = getLocalID();
            storageConfig.name = getName();
            Class<?> clazz = Class.forName(storageConfig.moduleClass);
            storage = (IRecordStorageModule<StorageConfig>)clazz.getDeclaredConstructor().newInstance();
            storage.setParentHub(getParentHub());
            storage.init(storageConfig);
            storage.start();
        }
        catch (Exception e)
        {
            throw new StorageException("Cannot instantiate underlying storage " + storageConfig.moduleClass, e);
        }
        
        // start auto-purge timer thread if policy is specified and enabled
        if (config.autoPurgeConfig != null && config.autoPurgeConfig.enabled)
        {
            final IStorageAutoPurgePolicy policy = config.autoPurgeConfig.getPolicy();
            autoPurgeTimer = new Timer();
            TimerTask task = new TimerTask() {
                public void run()
                {
                    policy.trimStorage(storage, logger);
                }
            };
            
            autoPurgeTimer.schedule(task, 0, (long)(config.autoPurgeConfig.purgePeriod*1000));
        }
        
        // retrieve reference to data source
        IDataProducer dataSource = getDataSource(config.dataSourceID);
        if (dataSource != null)
        {
            producerUID = dataSource.getUniqueIdentifier();
            
            // connect now if enabled
            if (dataSource.isEnabled())
                connectMainDataSource(dataSource);
            else
                reportStatus(WAITING_STATUS_MSG + producerUID);
        }
        
        // get notified if procedure is added later
        subscribeToProcedureRegistryEvents()
            .thenAccept(s -> procRegistrySub = s);
    }
    
    
    protected IDataProducer getDataSource(String procUID)
    {
        IProcedureWithState proc = getParentHub().getProcedureRegistry().get(procUID);
        if (!(proc instanceof IDataProducer))
            throw new IllegalStateException("Procedure " + procUID + " is not a data producer");
        return (IDataProducer)proc;
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
                if (dataSource.isEnabled())
                {
                    if (e instanceof ProcedureAddedEvent || e instanceof ProcedureEnabledEvent)
                    {
                        if (procID.equals(producerUID))
                            connectMainDataSource(dataSource);
                        else
                            ensureProducerInfo(procID);
                    }
                    else
                    {
                        disconnectDataSource(dataSource);
                        reportStatus("Disconnected for data source " + MsgUtils.entityString(dataSource));
                    }
                }
            });
    }
    
    
    /*
     * Gets the list of selected outputs (i.e. a subset of all data source outputs)
     */
    protected Collection<? extends IStreamingDataInterface> getSelectedOutputs(IDataProducer dataSource)
    {
        if (config.excludedOutputs == null || config.excludedOutputs.isEmpty())
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
        }
    }
    
    
    protected void connectMainDataSource(IDataProducer dataSource)
    {
        synchronized (dataSource)
        {                
            this.dataSourceRef = new WeakReference<>(dataSource);
            connectDataSource(dataSource, storage);
            clearStatus();
        }
    }
    
    
    /*
     * Connects to data source and store initial metadata for all selected streams
     */
    protected void connectDataSource(final IDataProducer dataSource, final IBasicStorage dataStore)
    {
        Asserts.checkNotNull(dataSource, IDataProducer.class);
        Asserts.checkNotNull(dataStore, "DataStore");
        
        // need to make sure we add things that are missing in storage
            
        // copy data source description if none was set
        if (dataStore.getLatestDataSourceDescription() == null)
            dataStore.storeDataSourceDescription(dataSource.getCurrentDescription());
        
        // otherwise just get the latest sensor description in case we were down during the last update
        else if (dataSource.getLastDescriptionUpdate() != Long.MIN_VALUE)
            dataStore.updateDataSourceDescription(dataSource.getCurrentDescription());
            
        // add record store for each selected output that is not already registered
        var selectedOutputs = getSelectedOutputs(dataSource);
        for (IStreamingDataInterface output: selectedOutputs)
        {
            if (!dataStore.getRecordStores().containsKey(output.getName()))
                dataStore.addRecordStore(output.getName(), output.getRecordDescription(), output.getRecommendedEncoding());
        }
        
        // init current FOI
        String producerID = dataSource.getUniqueIdentifier();
        IGeoFeature foi = dataSource.getCurrentFeatureOfInterest();
        if (foi != null)
        {
            currentFoiMap.put(producerID, foi.getUniqueIdentifier());
            if (dataStore instanceof IObsStorage)
                ((IObsStorage)dataStore).storeFoi(producerID, (AbstractFeature)foi);
        }
        
        // make sure changes are written to storage
        storage.commit();
        
        // register to output data events
        subscribeToProcedureEvents(dataSource, selectedOutputs);
                
        // if multisource, call recursively to connect nested producers
        if (dataSource instanceof IMultiSourceDataProducer && storage instanceof IMultiSourceStorage)
        {
            IMultiSourceDataProducer multiSource = (IMultiSourceDataProducer)dataSource;
            for (String nestedProducerID: multiSource.getMembers().keySet())
                ensureProducerInfo(nestedProducerID);
        }       
    }
    
    
    /*
     * Ensure producer data store has been initialized
     */
    protected void ensureProducerInfo(String producerID)
    {
        IDataProducer dataSource = dataSourceRef.get();
        if (dataSource instanceof IMultiSourceDataProducer && storage instanceof IMultiSourceStorage)
        {
            IDataProducer producer = ((IMultiSourceDataProducer)dataSource).getMembers().get(producerID);
            if (producer != null)
            {
                synchronized (producer)
                {
                    IBasicStorage dataStore = ((IMultiSourceStorage<?>)storage).getDataStore(producerID);
                    if (dataStore == null)
                        dataStore = ((IMultiSourceStorage<?>)storage).addDataStore(producerID);
                                
                    if (!registeredProducers.containsKey(producerID))
                        connectDataSource(producer, dataStore);
                }
            }
        }
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
        Subscription dataSub = registeredProducers.remove(dataSource.getUniqueIdentifier());
        if (dataSub != null)
            dataSub.cancel();
    }
    
    
    /*
     * Subscribe to data, foi and procedure changed events coming from the
     * specified producer
     */
    protected void subscribeToProcedureEvents(IDataProducer producer, Collection<? extends IStreamingDataInterface> selectedOutputs)
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
            // create time stamp indexer
            timeStampIndexers.computeIfAbsent(output.getName(),
                k -> SWEHelper.getTimeStampIndexer(output.getRecordDescription()));
            
            // fetch latest record
            DataBlock rec = output.getLatestRecord();
            if (rec != null)
                onNext(new DataEvent(System.currentTimeMillis(), output, rec));
        
            // add output to subscription
            subscription.withSource(output);
        }
        
        // finalize subscription
        subscription.subscribe(this::onNext, e -> {
                getLogger().error("Error during event dispatch", e);
            }, () -> {})
            .thenAccept(subAck -> {
                getLogger().debug("Connected to data source {}", producer.getUniqueIdentifier());
                registeredProducers.put(producer.getUniqueIdentifier(), subAck);
                subAck.request(Long.MAX_VALUE);
            });
    }
    
        
    @Override
    public synchronized void stop() throws SensorHubException
    {
        processEvents = false;
        
        if (dataSourceRef != null)
        {
            // unsubscribe from procedure registry
            if (procRegistrySub != null)
                procRegistrySub.cancel();
            
            // unsubscribe from all procedure
            IDataProducer dataSource = dataSourceRef.get();
            if (dataSource != null)
                disconnectDataSource(dataSource);
            
            dataSourceRef = null;
        }
        
        if (autoPurgeTimer != null)
            autoPurgeTimer.cancel();

        if (storage != null)
            storage.stop();
    }


    @Override
    public void cleanup() throws SensorHubException
    {
        if (storage != null)
            storage.cleanup();
        super.cleanup();
    }
    
    
    public void onNext(ProcedureEvent e)
    {
        if (processEvents)
        {
            // new data events
            if (e instanceof DataEvent)
            {
                DataEvent dataEvent = (DataEvent)e;
                String producerID = dataEvent.getProcedureUID();
                
                if (producerID != null && registeredProducers.containsKey(producerID))
                {
                    // get indexer for looking up time stamp value
                    String outputName = dataEvent.getSource().getName();
                    ScalarIndexer timeStampIndexer = timeStampIndexers.get(outputName);
                    
                    // get FOI ID
                    String foiID = currentFoiMap.get(producerID);
                    
                    // store all records
                    for (DataBlock record: dataEvent.getRecords())
                    {
                        // get time stamp
                        double time;
                        if (timeStampIndexer != null)
                            time = timeStampIndexer.getDoubleValue(record);
                        else
                            time = e.getTimeStamp() / 1000.;
                    
                        // store record with proper key
                        ObsKey key = new ObsKey(outputName, producerID, foiID, time);                    
                        storage.storeRecord(key, record);
                        
                        getLogger().trace("Storing record {} for output {}", key.timeStamp, outputName);
                    }
                }
            }
            
            else if (e instanceof FoiEvent && storage instanceof IObsStorage)
            {
                FoiEvent foiEvent = (FoiEvent)e;
                String producerID = ((FoiEvent) e).getProcedureUID();
                
                if (producerID != null && registeredProducers.containsKey(producerID))
                {
                    // store feature object if specified
                    if (foiEvent.getFoi() != null)
                    {
                        ((IObsStorage) storage).storeFoi(producerID, (AbstractFeature)foiEvent.getFoi());
                        getLogger().trace("Storing FOI {}", foiEvent.getFoiUID());
                    }
                
                    // also remember as current FOI
                    currentFoiMap.put(producerID, foiEvent.getFoiUID());
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
                storage.storeDataSourceDescription(dataSourceRef.get().getCurrentDescription());
            }
            
            // commit only when necessary
            long now = System.currentTimeMillis();
            if (lastCommitTime == Long.MIN_VALUE || (now - lastCommitTime) > config.minCommitPeriod)
            {
                storage.commit();
                lastCommitTime = now;
            }
        }
    }
    

    @Override
    public void addRecordStore(String name, DataComponent recordStructure, DataEncoding recommendedEncoding)
    {
        checkStarted();
        
        // register new record type with underlying storage
        if (!storage.getRecordStores().containsKey(name))
            storage.addRecordStore(name, recordStructure, recommendedEncoding);
        
        /*// prepare to receive events
        IDataProducer dataSource = dataSourceRef.get();
        if (dataSource != null)
            prepareToReceiveEvents(dataSource.getOutputs().get(name));*/
    }


    @Override
    public void backup(OutputStream os) throws IOException
    {
        checkStarted();
        storage.backup(os);        
    }


    @Override
    public void restore(InputStream is) throws IOException
    {
        checkStarted();
        storage.restore(is);        
    }


    @Override
    public void commit()
    {
        checkStarted();
        storage.commit();        
    }


    @Override
    public void rollback()
    {
        checkStarted();
        storage.rollback();        
    }


    @Override
    public void sync(IStorageModule<?> storage) throws StorageException
    {
        checkStarted();
        this.storage.sync(storage);        
    }


    @Override
    public AbstractProcess getLatestDataSourceDescription()
    {
        checkStarted();
        return storage.getLatestDataSourceDescription();
    }


    @Override
    public List<AbstractProcess> getDataSourceDescriptionHistory(double startTime, double endTime)
    {
        checkStarted();
        return storage.getDataSourceDescriptionHistory(startTime, endTime);
    }


    @Override
    public AbstractProcess getDataSourceDescriptionAtTime(double time)
    {
        checkStarted();
        return storage.getDataSourceDescriptionAtTime(time);
    }


    @Override
    public void storeDataSourceDescription(AbstractProcess process)
    {
        checkStarted();
        storage.storeDataSourceDescription(process);        
    }


    @Override
    public void updateDataSourceDescription(AbstractProcess process)
    {
        checkStarted();
        storage.updateDataSourceDescription(process);        
    }


    @Override
    public void removeDataSourceDescription(double time)
    {
        checkStarted();
        storage.removeDataSourceDescription(time);        
    }


    @Override
    public void removeDataSourceDescriptionHistory(double startTime, double endTime)
    {
        checkStarted();
        storage.removeDataSourceDescriptionHistory(startTime, endTime);
    }


    @Override
    public Map<String, ? extends IRecordStoreInfo> getRecordStores()
    {
        checkStarted();
        return storage.getRecordStores();
    }


    @Override
    public DataBlock getDataBlock(DataKey key)
    {
        checkStarted();
        return storage.getDataBlock(key);
    }


    @Override
    public Iterator<DataBlock> getDataBlockIterator(IDataFilter filter)
    {
        checkStarted();
        return storage.getDataBlockIterator(filter);
    }


    @Override
    public Iterator<? extends IDataRecord> getRecordIterator(IDataFilter filter)
    {
        checkStarted();
        return storage.getRecordIterator(filter);
    }


    @Override
    public int getNumMatchingRecords(IDataFilter filter, long maxCount)
    {
        checkStarted();
        return storage.getNumMatchingRecords(filter, maxCount);
    }

    
    @Override
    public int getNumRecords(String recordType)
    {
        checkStarted();
        return storage.getNumRecords(recordType);
    }


    @Override
    public double[] getRecordsTimeRange(String recordType)
    {
        checkStarted();
        return storage.getRecordsTimeRange(recordType);
    }
    
    
    @Override
    public int[] getEstimatedRecordCounts(String recordType, double[] timeStamps)
    {
        checkStarted();
        return storage.getEstimatedRecordCounts(recordType, timeStamps);
    }


    @Override
    public void storeRecord(DataKey key, DataBlock data)
    {
        checkStarted();
        storage.storeRecord(key, data);
    }


    @Override
    public void updateRecord(DataKey key, DataBlock data)
    {
        checkStarted();
        storage.updateRecord(key, data);
    }


    @Override
    public void removeRecord(DataKey key)
    {
        checkStarted();
        storage.removeRecord(key);
    }


    @Override
    public int removeRecords(IDataFilter filter)
    {
        checkStarted();
        return storage.removeRecords(filter);
    }


    @Override
    public int getNumFois(IFoiFilter filter)
    {
        checkStarted();
        
        if (storage instanceof IObsStorage)
            return ((IObsStorage) storage).getNumFois(filter);
        
        return 0;
    }
    
    
    @Override
    public Bbox getFoisSpatialExtent()
    {
        checkStarted();
        
        if (storage instanceof IObsStorage)
            return ((IObsStorage) storage).getFoisSpatialExtent();
        
        return null;
    }


    @Override
    public Iterator<String> getFoiIDs(IFoiFilter filter)
    {
        checkStarted();
        
        if (storage instanceof IObsStorage)
            return ((IObsStorage) storage).getFoiIDs(filter);
        
        return Collections.emptyIterator();
    }


    @Override
    public Iterator<AbstractFeature> getFois(IFoiFilter filter)
    {
        checkStarted();
        
        if (storage instanceof IObsStorage)
            return ((IObsStorage) storage).getFois(filter);
        
        return Collections.emptyIterator();
    }


    @Override
    public void storeFoi(String producerID, AbstractFeature foi)
    {
        checkStarted();
        if (storage instanceof IObsStorage)
            storeFoi(producerID, foi);        
    }
    
    
    @Override
    public Collection<String> getProducerIDs()
    {
        checkStarted();
        
        if (storage instanceof IMultiSourceStorage)
            return ((IMultiSourceStorage<?>)storage).getProducerIDs();
        else
            return Collections.emptyList();
    }


    @Override
    public IObsStorage getDataStore(String producerID)
    {
        checkStarted();
        
        if (storage instanceof IMultiSourceStorage)
            return ((IMultiSourceStorage<IObsStorage>)storage).getDataStore(producerID);
        else
            return null;
    }


    @Override
    public IObsStorage addDataStore(String producerID)
    {
        checkStarted();
        
        if (storage instanceof IMultiSourceStorage)
            return ((IMultiSourceStorage<IObsStorage>)storage).addDataStore(producerID);
        else
            return null;
    }
    
    
    private void checkStarted()
    {
        if (storage == null)
            throw new IllegalStateException("Storage is disabled");
    }


    @Override
    protected void setState(ModuleState newState)
    {
        // switch to started only if we already have data in storage
        // otherwise we have to wait for data source to start
        if (newState == ModuleState.STARTED && storage.getLatestDataSourceDescription() == null)
            return;
            
        super.setState(newState);
    }


    @Override
    public boolean isReadSupported()
    {
        checkStarted();
        return storage.isReadSupported();
    }


    @Override
    public boolean isWriteSupported()
    {
        return true;
    }
    
    
    public boolean isMultiSource()
    {
        checkStarted();
        return storage instanceof IMultiSourceStorage;
    }
}
