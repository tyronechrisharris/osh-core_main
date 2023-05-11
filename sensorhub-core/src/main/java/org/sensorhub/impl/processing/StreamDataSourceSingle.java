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

import java.util.concurrent.TimeoutException;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.processing.OSHProcessInfo;
import org.sensorhub.utils.Async;
import org.vast.process.ProcessInfo;
import org.vast.swe.SWEHelper;
import net.opengis.swe.v20.Text;


/**
 * <p>
 * Process implementation used to feed data from a single data stream
 * (received from event bus) into a SensorML processing chain.
 * </p>
 *
 * @author Alex Robin
 * @since May 10, 2023
 */
public class StreamDataSourceSingle extends StreamDataSource
{
    public static final OSHProcessInfo INFO = new OSHProcessInfo("datasource:stream", "DataStream Source", null, StreamDataSourceSingle.class);
    public static final String OUTPUT_NAME_PARAM = "systemOutput";
    
    Text outputNameParam;
    String outputName;
    
    
    public StreamDataSourceSingle()
    {
        SWEHelper fac = new SWEHelper();
        
        outputNameParam = fac.createText()
            .definition(SWEHelper.getPropertyUri("OutputName"))
            .label("Output Name")
            .build();
        paramData.add(OUTPUT_NAME_PARAM, outputNameParam);
        
        // output cannot be created until source URI param is set
    }


    @Override
    public void notifyParamChange()
    {
        producerUri = producerUriParam.getData().getStringValue();
        outputName = outputNameParam.getData().getStringValue();
        
        if (producerUri != null && outputName != null)
        {
            try {
                // wait here to make sure datasource and its datastreams have been registered.
                // needed to handle case where datasource is being registered concurrently.
                Async.waitForCondition(this::checkForDataSource, 500, 10000);
            } catch (TimeoutException e) {
                if (processInfo == null)
                    throw new IllegalStateException("System " + producerUri + " not found", e);
                else
                    throw new IllegalStateException("System " + producerUri + " is missing output " + outputName, e);
            }
        }
    }
    
    
    protected boolean checkForDataSource()
    {
        var db = hub.getDatabaseRegistry().getFederatedDatabase();
        var sysEntry = db.getSystemDescStore().getCurrentVersionEntry(producerUri);
        if (sysEntry == null)
            return false;
        
        // set process info
        ProcessInfo instanceInfo = new ProcessInfo(
                processInfo.getUri(),
                sysEntry.getValue().getName(),
                processInfo.getDescription(),
                processInfo.getImplementationClass());
        this.processInfo = instanceInfo;
        
        // get datastream corresponding to outputName
        db.getDataStreamStore().select(new DataStreamFilter.Builder()
                .withSystems(sysEntry.getKey().getInternalID())
                .withOutputNames(outputName)
                .withCurrentVersion()
                .build())
            .forEach(ds -> {
                outputData.add(ds.getOutputName(), ds.getRecordStructure().copy());
            });
        
        return !outputData.isEmpty();
    }

}
