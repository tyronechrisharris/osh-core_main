/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.processing;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.opengis.OgcPropertyList;
import net.opengis.swe.v20.AbstractSWEIdentifiable;
import net.opengis.swe.v20.DataComponent;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.processing.ProcessingException;
import org.sensorhub.api.utils.OshAsserts;
import org.vast.process.ProcessException;
import org.vast.sensorML.AggregateProcessImpl;
import org.vast.sensorML.SMLException;
import org.vast.sensorML.SMLHelper;
import org.vast.sensorML.SMLUtils;


/**
 * <p>
 * Implementation of processing API using the SensorML format for describing
 * the process flow and the SensorML execution engine to run the process.
 * </p>
 *
 * @author Alex Robin
 * @since Feb 22, 2015
 */
public class SMLProcessImpl extends AbstractProcessModule<SMLProcessConfig>
{
    public static final String DEFAULT_ID = "PROCESS_DESC";
    protected static final String DATASRC_NAME = "datasource_";
    protected static final String DATASINK_NAME = "datasink_";
    protected static final String PROCESS_NAME = "main_process";
    protected static final int MAX_ERRORS = 10;
    
    protected SMLUtils smlUtils;
    protected List<StreamDataSource> streamSources;
    protected AggregateProcessImpl wrapperProcess;
    protected long lastUpdatedProcess = Long.MIN_VALUE;
    protected boolean paused = false;
    protected int errorCount = 0;
    
    
    public SMLProcessImpl()
    {
        wrapperProcess = new AggregateProcessImpl();
        wrapperProcess.setUniqueIdentifier(UUID.randomUUID().toString());
        initAsync = true;
    }
    
    
    @Override
    public void setParentHub(ISensorHub hub)
    {
        super.setParentHub(hub);
        smlUtils = new SMLUtils(SMLUtils.V2_0);
        smlUtils.setProcessFactory(hub.getProcessingManager());
    }


    @Override
    protected void doInit() throws SensorHubException
    {
        // only go further if sensorML file was provided
        // otherwise we'll do it at the next update
        if (config.sensorML != null)
        {
            String smlPath = config.getSensorMLPath();
            
            // parse SensorML file
            try (InputStream is = new BufferedInputStream(new FileInputStream(smlPath)))
            {
                processDescription = (AggregateProcessImpl)smlUtils.readProcess(is);
                OshAsserts.checkSystemObject(processDescription);
                
                // set default name if none set in SensorML file
                if (processDescription.getName() == null)
                    processDescription.setName(this.getName());
                
                initChain();
            }
            catch (Exception e)
            {
                throw new ProcessingException(String.format("Cannot read SensorML description from '%s'", smlPath), e);
            }
            
            /*CompletableFuture.runAsync(Lambdas.checked(() -> initChain()))
                .exceptionally(e -> {
                    reportError("Error initializing SML process", e);
                    return null;
                });*/
        }
    }
    
    
    protected void initChain() throws SensorHubException
    {
        // make process executable
        try
        {
            //smlUtils.makeProcessExecutable(wrapperProcess, true);
            wrapperProcess = (AggregateProcessImpl)smlUtils.getExecutableInstance((AggregateProcessImpl)processDescription, true);
            wrapperProcess.setInstanceName("chain");
            wrapperProcess.setParentLogger(getLogger());
            wrapperProcess.init();
        }
        catch (SMLException e)
        {
            throw new ProcessingException("Cannot prepare process chain for execution", e);
        }
        catch (ProcessException e)
        {
            throw new ProcessingException(e.getMessage(), e.getCause());
        }
        
        // advertise process inputs and outputs
        refreshIOList(processDescription.getInputList(), inputs, false);
        refreshIOList(processDescription.getParameterList(), parameters, false);
        refreshIOList(processDescription.getOutputList(), outputs, true);
        
        setState(ModuleState.INITIALIZED);
    }
    
    
    public AggregateProcessImpl getProcessChain()
    {
        return wrapperProcess;
    }
    
    
    protected void refreshIOList(OgcPropertyList<AbstractSWEIdentifiable> ioList, Map<String, DataComponent> ioMap, boolean isOutput) throws ProcessingException
    {
        ioMap.clear();
        if (isOutput)
            outputInterfaces.clear();
                
        int numSignals = ioList.size();
        for (int i=0; i<numSignals; i++)
        {
            String ioName = ioList.getProperty(i).getName();
            AbstractSWEIdentifiable ioDesc = ioList.get(i);
            
            DataComponent ioComponent = SMLHelper.getIOComponent(ioDesc);
            ioMap.put(ioName, ioComponent);
            
            if (isOutput)
                outputInterfaces.put(ioName, new SMLOutputInterface(this, ioDesc));
        }
    }
    
    
    @Override
    protected void doStart() throws SensorHubException
    {
        errorCount = 0;
        
        if (wrapperProcess == null)
            throw new ProcessingException("No valid processing chain provided");
        
        // start processing thread
        try
        {
            wrapperProcess.start(e-> {
                reportError("Error while executing process chain", e);
            });
        }
        catch (ProcessException e)
        {
            throw new ProcessingException("Cannot start process chain thread", e);
        }
    }
    
    
    @Override
    protected void doStop()
    {
        if (wrapperProcess != null && wrapperProcess.isExecutable())
            wrapperProcess.stop();
    }
}
