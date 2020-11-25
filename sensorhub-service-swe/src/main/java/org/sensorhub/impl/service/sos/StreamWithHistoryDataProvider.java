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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import org.sensorhub.api.data.DataEvent;
import org.sensorhub.impl.service.swe.RecordTemplate;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.om.IObservation;
import org.vast.ogc.om.IProcedure;
import org.vast.ows.sos.GetFeatureOfInterestRequest;
import org.vast.ows.sos.GetObservationRequest;
import org.vast.ows.sos.GetResultRequest;
import org.vast.ows.sos.SOSException;
import org.vast.ows.sos.SOSOfferingCapabilities;
import org.vast.ows.swe.DescribeSensorRequest;
import org.vast.util.TimeExtent;


/**
 * <p>
 * Special provider to collect latest records either from real-time source
 * or storage. For each producer, we favor the latest record coming from the
 * real-time source when available and fall back on storage if not.
 * </p>
 *
 * @author Alex Robin
 * @since Jun 6, 2019
 */
public class StreamWithHistoryDataProvider implements ISOSAsyncDataProvider
{
    final SOSService service;
    final String procUID;
    final StreamingDataProviderConfig config;
        
    
    public StreamWithHistoryDataProvider(final SOSService service, final String procUID, final StreamingDataProviderConfig config)
    {
        this.service = service;
        this.procUID = procUID;
        this.config = config;
    }
    
    
    ISOSAsyncDataProvider selectProvider(TimeExtent requestedTime)
    {
        
    }


    @Override
    public CompletableFuture<SOSOfferingCapabilities> getCapabilities() throws SOSException, IOException
    {
        return new HistoricalDataProvider(service, procUID, null).getCapabilities();
        
        // TODO just update time in capabilities to include 'now'
    }


    @Override
    public CompletableFuture<SOSOfferingCapabilities> updateCapabilities(SOSOfferingCapabilities caps) throws SOSException, IOException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public CompletableFuture<RecordTemplate> getResultTemplate(Set<String> observables) throws SOSException, IOException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void getResults(GetResultRequest req, Subscriber<DataEvent> consumer) throws SOSException, IOException
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    public void getObservations(GetObservationRequest req, Subscriber<IObservation> consumer) throws SOSException, IOException
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    public void getProcedureDescriptions(DescribeSensorRequest req, Subscriber<IProcedure> consumer) throws SOSException, IOException
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    public void getFeaturesOfInterest(GetFeatureOfInterestRequest req, Subscriber<IGeoFeature> consumer) throws SOSException, IOException
    {
        // TODO Auto-generated method stub
        
    }


    @Override
    public boolean hasMultipleProducers()
    {
        // TODO Auto-generated method stub
        return false;
    }


    @Override
    public String getProducerIDPrefix()
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void close()
    {
        // TODO Auto-generated method stub
        
    }
}
