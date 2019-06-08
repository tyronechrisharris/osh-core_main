/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.io.IOException;
import java.util.HashSet;
import org.sensorhub.api.data.IDataProducerModule;
import org.sensorhub.api.persistence.IBasicStorage;
import org.sensorhub.api.persistence.IMultiSourceStorage;
import org.vast.ogc.om.IObservation;
import org.vast.ows.OWSException;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Special provider to collect latest records either from real-time source
 * or storage. For each producer, we favor the latest record coming from the
 * real-time source when available and fall back on storage if not.
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Jun 6, 2019
 */
public class StreamWithStorageDataProvider implements ISOSDataProvider
{
    StreamDataProvider streamProvider;
    StorageDataProvider storageProvider;
    HashSet<String> producersRead = new HashSet<>();
    int numProducers = 1;
    boolean readingFromStorage;
        
    
    @SuppressWarnings("rawtypes")
    public StreamWithStorageDataProvider(IDataProducerModule<?> producer, IBasicStorage storage, StreamDataProviderConfig config, final SOSDataFilter filter) throws OWSException
    {
        this.streamProvider = new StreamDataProvider(producer, config, filter);
        this.storageProvider = new StorageDataProvider(storage, new StorageDataProviderConfig(config), filter);
        
        if (storage instanceof IMultiSourceStorage)
            this.numProducers = ((IMultiSourceStorage)storage).getProducerIDs().size() - 1;
    }
    
    
    @Override
    public IObservation getNextObservation() throws IOException
    {
        DataBlock rec = getNextResultRecord();
        if (rec == null)
            return null;
        
        return buildObservation(rec);
    }
    
    
    protected IObservation buildObservation(DataBlock rec) throws IOException
    {
        if (readingFromStorage)
            return storageProvider.buildObservation(rec);
        else
            return streamProvider.buildObservation(rec);
    }


    @Override
    public DataBlock getNextResultRecord() throws IOException
    {
        DataBlock rec = null;
        
        if (!readingFromStorage)
        {
            // first send latest records available from stream source
            rec = streamProvider.getNextResultRecord();
            if (rec != null)
            {
                producersRead.add(streamProvider.lastDataEvent.getRelatedEntityID());
                return rec;
            }
            
            readingFromStorage = true;
        }
        
        while (producersRead.size() < numProducers)
        {
            // if some are missing, complete with latest records from storage
            // this can happen after a hub restart or because some producers are not 
            // available anymore from real-time source, but data still in storage
            rec = storageProvider.getNextResultRecord();
            String producerID = storageProvider.lastRecordKey.producerID;
            if (rec == null || !producersRead.contains(producerID))
            {
                producersRead.add(producerID);
                return rec;
            }
        }
        
        return null;
    }


    @Override
    public DataComponent getResultStructure() throws IOException
    {
        return streamProvider.getResultStructure();
    }


    @Override
    public DataEncoding getDefaultResultEncoding() throws IOException
    {
        return streamProvider.getDefaultResultEncoding();
    }


    @Override
    public void close()
    {
        streamProvider.close();
        storageProvider.close();
    }

}
