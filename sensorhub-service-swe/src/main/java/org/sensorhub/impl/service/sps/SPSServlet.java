/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sps;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.namespace.QName;
import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataComponent;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.server.WebSocketServerFactory;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.sensorhub.api.common.IEventHandler;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.ModuleConfig;
import org.sensorhub.api.module.ModuleEvent.ModuleState;
import org.sensorhub.api.security.ISecurityManager;
import org.sensorhub.impl.SensorHub;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.sensor.swe.ITaskingCallback;
import org.sensorhub.impl.service.ogc.OGCServiceConfig.CapabilitiesInfo;
import org.sensorhub.impl.service.swe.Template;
import org.sensorhub.impl.service.swe.TransactionUtils;
import org.slf4j.Logger;
import org.vast.cdm.common.DataStreamWriter;
import org.vast.data.DataBlockList;
import org.vast.ows.GetCapabilitiesRequest;
import org.vast.ows.OWSExceptionReport;
import org.vast.ows.OWSRequest;
import org.vast.ows.server.OWSServlet;
import org.vast.ows.sps.*;
import org.vast.ows.sps.StatusReport.RequestStatus;
import org.vast.ows.sps.StatusReport.TaskStatus;
import org.vast.ows.swe.DescribeSensorRequest;
import org.vast.ows.util.PostRequestFilter;
import org.vast.sensorML.SMLUtils;
import org.vast.swe.SWEHelper;
import org.vast.xml.DOMHelper;
import org.w3c.dom.Element;


/**
 * <p>
 * Extension of OWSSServlet implementing Sensor Planning Service operations
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Jan 15, 2015
 */
@SuppressWarnings("serial")
public class SPSServlet extends OWSServlet
{
    private static final String INVALID_WS_REQ_MSG = "Invalid Websocket request: ";        
    protected static final String DEFAULT_VERSION = "2.0.0";
    private static final QName EXT_WS = new QName("websocket");
    
    SPSServiceConfig config;
    SPSSecurity securityHandler;
    Logger log;
    String endpointUrl;
    WebSocketServletFactory factory = new WebSocketServerFactory();
    ReentrantReadWriteLock capabilitiesLock = new ReentrantReadWriteLock();
    SPSServiceCapabilities capabilities;
    Map<String, ISPSConnector> connectors = new HashMap<String, ISPSConnector>(); // key is procedure ID
    Map<String, SPSOfferingCapabilities> offeringCaps = new HashMap<String, SPSOfferingCapabilities>(); // key is procedure ID
    Map<String, String> templateToProcedureMap = new HashMap<String, String>();
    IEventHandler eventHandler;
        
    SMLUtils smlUtils = new SMLUtils(SMLUtils.V2_0);
    ITaskDB taskDB;
    //SPSNotificationSystem notifSystem;
    
    ModuleState state;
    
    
    public SPSServlet(SPSServiceConfig config, SPSSecurity securityHandler, Logger log)
    {
        this.config = config;
        this.securityHandler = securityHandler;
        this.log = log;
        this.owsUtils = new SPSUtils();
    }
    
    
    protected void start() throws SensorHubException
    {
        this.taskDB = new InMemoryTaskDB();
        
        // pre-generate capabilities
        endpointUrl = null;
        generateCapabilities();       
    }
    
    
    protected void stop()
    {
        // clean all connectors
        for (ISPSConnector connector: connectors.values())
            connector.cleanup();
    }
    
    
    @Override
    public void destroy()
    {
        stop();
    }
    
    
    /**
     * Generates the SPSServiceCapabilities object with info obtained from connector
     */
    protected void generateCapabilities()
    {
        connectors.clear();
        offeringCaps.clear();
        capabilities = new SPSServiceCapabilities();
        
        // get main capabilities info from config
        CapabilitiesInfo serviceInfo = config.ogcCapabilitiesInfo;
        capabilities.getIdentification().setTitle(serviceInfo.title);
        capabilities.getIdentification().setDescription(serviceInfo.description);
        capabilities.setFees(serviceInfo.fees);
        capabilities.setAccessConstraints(serviceInfo.accessConstraints);
        capabilities.setServiceProvider(serviceInfo.serviceProvider);
        
        // supported operations
        capabilities.getGetServers().put("GetCapabilities", config.endPoint);
        capabilities.getGetServers().put("DescribeSensor", config.endPoint);
        capabilities.getPostServers().putAll(capabilities.getGetServers());
        capabilities.getPostServers().put("Submit", config.endPoint);
        
        if (config.enableTransactional)
        {
            //capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_SENSOR_INSERTION);
            //capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_SENSOR_DELETION);/
            capabilities.getPostServers().put("InsertSensor", config.endPoint);
            capabilities.getPostServers().put("DeleteSensor", config.endPoint);
            capabilities.getPostServers().put("InsertTaskingTemplate", config.endPoint);
            capabilities.getGetServers().put("ConnectTaskingTemplate", config.endPoint);
        }
        
        // generate profile list
        /*capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_RESULT_RETRIEVAL);
        if (config.enableTransactional)
        {
            capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_RESULT_INSERTION);
            capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_OBS_INSERTION);
        }*/
        
        // process each provider config
        if (config.connectors != null)
        {
            for (SPSConnectorConfig connectorConf: config.connectors)
            {
                try
                {
                    // instantiate provider factories and map them to offering URIs
                    ISPSConnector connector = connectorConf.getConnector(this);
                    connectors.put(connector.getProcedureID(), connector);
                    showConnectorCaps(connector);
                }
                catch (Exception e)
                {
                    log.error("Error while initializing connector " + connectorConf.offeringID, e);
                }
            }
        }
    }
    
    
    protected synchronized void showConnectorCaps(ISPSConnector connector)
    {
        SPSConnectorConfig config = connector.getConfig();
        
        try
        {
            capabilitiesLock.writeLock().lock();
            
            // generate offering metadata
            SPSOfferingCapabilities offCaps = connector.generateCapabilities();
            String procedureID = offCaps.getMainProcedure();
            
            // update offering if it was already advertised
            if (offeringCaps.containsKey(procedureID))
            {
                // replace old offering
                SPSOfferingCapabilities oldCaps = offeringCaps.put(procedureID, offCaps);
                capabilities.getLayers().set(capabilities.getLayers().indexOf(oldCaps), offCaps);
                
                if (log.isDebugEnabled())
                    log.debug("Offering " + "\"" + offCaps.getIdentifier() + "\" updated for procedure " + procedureID);
            }
            
            // otherwise add new offering
            else
            {
                // add to maps and layer list
                offeringCaps.put(procedureID, offCaps);
                capabilities.getLayers().add(offCaps);
                
                if (log.isDebugEnabled())
                    log.debug("Offering " + "\"" + offCaps.getIdentifier() + "\" added for procedure " + procedureID);
            }            
        }
        catch (Exception e)
        {
            log.error("Error while generating offering " + config.offeringID, e);
        }
        finally
        {
            capabilitiesLock.writeLock().unlock();
        }
    }
    
    
    protected synchronized void hideConnectorCaps(ISPSConnector connector)
    {
        try
        {
            capabilitiesLock.writeLock().lock();
            
            // get procedure ID
            String procID = connector.getProcedureID();
            if (procID == null)
                return;
            
            // remove offering from capabilities
            SPSOfferingCapabilities offCaps = offeringCaps.remove(procID);
            capabilities.getLayers().remove(offCaps);
            
            if (log.isDebugEnabled())
                log.debug("Offering " + "\"" + offCaps.getIdentifier() + "\" removed for procedure " + procID);
        }
        finally
        {
            capabilitiesLock.writeLock().unlock();
        }
    }
    
    
    /*
     * Completely removes a connector and corresponding offering
     * This is called when the tasking target is deleted
     */
    protected synchronized void removeConnector(String procedureID)
    {
        String offeringID = getOfferingID(procedureID);
        
        // delete connector
        ISPSConnector connector = connectors.remove(procedureID);
        if (connector != null)
        {
            hideConnectorCaps(connector);
            connector.cleanup();
        }
        
        // delete connector config
        Iterator<SPSConnectorConfig> it = config.connectors.iterator();
        while (it.hasNext())
        {
            if (offeringID.equals(it.next().offeringID))
                it.remove();
        }
    }
    
    
    @Override
    protected OWSRequest parseRequest(HttpServletRequest req, HttpServletResponse resp, boolean post) throws Exception
    {
        // set current authentified user
        if (req.getRemoteUser() != null)
            securityHandler.setCurrentUser(req.getRemoteUser());
        else
            securityHandler.setCurrentUser(ISecurityManager.ANONYMOUS_USER);
        
        // reject request early if SPS not authorized at all
        securityHandler.checkPermission(securityHandler.rootPerm);
        
        try
        {
            OWSRequest owsRequest;
            
            if (post)
            {
                InputStream xmlRequest = new PostRequestFilter(new BufferedInputStream(req.getInputStream()));
                DOMHelper dom = new DOMHelper(xmlRequest, false);            
                Element requestElt = dom.getBaseElement();
                
                // detect and skip SOAP envelope if present
                String soapVersion = getSoapVersion(dom);
                if (soapVersion != null)
                    requestElt = getSoapBody(dom);
                
                // case of tasking request, need to get tasking params for the selected procedure
                if (isTaskingRequest(requestElt))
                {
                    String procID = dom.getElementValue(requestElt, "procedure");
                    SPSOfferingCapabilities offering = offeringCaps.get(procID);
                    if (offering == null)
                        throw new SPSException(SPSException.invalid_param_code, "procedure", procID);
                    DescribeTaskingResponse paramDesc = offering.getParametersDescription();
                    
                    // use full tasking params or updatable subset
                    DataComponent taskingParams;
                    if (requestElt.getLocalName().equals("Update"))
                        taskingParams = paramDesc.getUpdatableParameters();
                    else
                        taskingParams = paramDesc.getTaskingParameters();
                    
                    owsRequest = ((SPSUtils)owsUtils).readSpsRequest(dom, requestElt, taskingParams);
                }
                
                // case of normal request
                else
                    owsRequest = owsUtils.readXMLQuery(dom, requestElt);
                
                // keep http objects in request
                if (owsRequest != null)
                {
                    if (soapVersion != null)
                        owsRequest.setSoapVersion(soapVersion);
                    owsRequest.setHttpRequest(req);
                    owsRequest.setHttpResponse(resp);
                }
            }
            else
            {
                owsRequest = super.parseRequest(req, resp, post);
            }
            
            // detect websocket request
            if (factory.isUpgradeRequest(req, resp))
            {
                if (owsRequest instanceof ConnectTaskingRequest)
                    owsRequest.getExtensions().put(EXT_WS, true);
                else
                    throw new SPSException(INVALID_WS_REQ_MSG + owsRequest.getOperation() + " is not supported via this protocol");
            }
            
            return owsRequest;
        }
        finally
        {
            securityHandler.clearCurrentUser();
        }
    }
    
    
    protected void acceptWebSocket(final OWSRequest owsReq, final WebSocketListener socket) throws IOException
    {
        factory.acceptWebSocket(new WebSocketCreator() {
            @Override
            public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp)
            {
                return socket;
            }            
        }, owsReq.getHttpRequest(), owsReq.getHttpResponse());
    }
    
    
    @Override
    protected void handleRequest(OWSRequest request) throws Exception
    {
        if (request instanceof GetCapabilitiesRequest)
            handleRequest((GetCapabilitiesRequest)request);
        else if (request instanceof DescribeSensorRequest)
            handleRequest((DescribeSensorRequest)request);
        else if (request instanceof DescribeTaskingRequest)
            handleRequest((DescribeTaskingRequest)request);
        else if (request instanceof GetStatusRequest)
            handleRequest((GetStatusRequest)request);
        else if (request instanceof GetFeasibilityRequest)
            handleRequest((GetFeasibilityRequest)request);
        else if (request instanceof SubmitRequest)
            handleRequest((SubmitRequest)request);
        else if (request instanceof UpdateRequest)
            handleRequest((UpdateRequest)request);
        else if (request instanceof CancelRequest)
            handleRequest((CancelRequest)request);
        else if (request instanceof ReserveRequest)
            handleRequest((ReserveRequest)request);
        else if (request instanceof ConfirmRequest)
            handleRequest((ConfirmRequest)request);
        else if (request instanceof DescribeResultAccessRequest)
            handleRequest((DescribeResultAccessRequest)request);
        
        // transactional operations
        else if (request instanceof InsertSensorRequest)
            handleRequest((InsertSensorRequest)request);
        else if (request instanceof InsertTaskingTemplateRequest)
            handleRequest((InsertTaskingTemplateRequest)request);
        else if (request instanceof ConnectTaskingRequest)
            handleRequest((ConnectTaskingRequest)request);
        
        else
            throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    
    
    protected boolean isTaskingRequest(Element requestElt)
    {
        String localName = requestElt.getLocalName();
        
        if (localName.equals("GetFeasibility"))
            return true;
        else if (localName.equals("Submit"))
            return true;
        else if (localName.equals("Update"))
            return true;
        else if (localName.equals("Reserve"))
            return true;
        
        return false;
    }
    
    
    protected void handleRequest(GetCapabilitiesRequest request) throws Exception
    {
        /*// check that version 2.0.0 is supported by client
        if (!request.getAcceptedVersions().isEmpty())
        {
            if (!request.getAcceptedVersions().contains(DEFAULT_VERSION))
                throw new SOSException(SOSException.version_nego_failed_code, "AcceptVersions", null,
                        "Only version " + DEFAULT_VERSION + " is supported by this server");
        }
        
        // set selected version
        request.setVersion(DEFAULT_VERSION);*/
        
        // security check
        securityHandler.checkPermission(securityHandler.sps_read_caps);
        
        // update operation URLs
        if (endpointUrl == null)
        {
            try
            {
                capabilitiesLock.writeLock().lock();
            
                endpointUrl = request.getHttpRequest().getRequestURL().toString();
                for (Entry<String, String> op: capabilities.getGetServers().entrySet())
                    capabilities.getGetServers().put(op.getKey(), endpointUrl);
                for (Entry<String, String> op: capabilities.getPostServers().entrySet())
                    capabilities.getPostServers().put(op.getKey(), endpointUrl);
            }
            finally
            {            
                capabilitiesLock.writeLock().unlock();
            }
        }
            
        try
        {
            capabilitiesLock.readLock().lock();
            sendResponse(request, capabilities);
        }
        finally
        {            
            capabilitiesLock.readLock().unlock();
        }
    }
    
    
    protected void handleRequest(DescribeSensorRequest request) throws Exception
    {
        String procID = request.getProcedureID();
        
        OWSExceptionReport report = new OWSExceptionReport();
        ISPSConnector connector = getConnector(procID);
        checkQueryProcedureFormat(procID, request.getFormat(), report);
        report.process();
        
        // security check
        securityHandler.checkPermission(getOfferingID(procID), securityHandler.sps_read_sensor);
        
        // serialize and send SensorML description
        OutputStream os = new BufferedOutputStream(request.getResponseStream());
        smlUtils.writeProcess(os, connector.generateSensorMLDescription(Double.NaN), true);
    }
    
    
    protected void handleRequest(DescribeTaskingRequest request) throws Exception
    {
        String procID = request.getProcedureID();
        SPSOfferingCapabilities offering = offeringCaps.get(procID);
        
        if (offering == null)
            throw new SPSException(SPSException.invalid_param_code, "procedure", procID);
        
        // security check
        securityHandler.checkPermission(offering.getIdentifier(), securityHandler.sps_read_params);
        
        sendResponse(request, offering.getParametersDescription());
    }
    
    
    protected ITask findTask(String taskID) throws SPSException
    {
        ITask task = taskDB.getTask(taskID);
        
        if (task == null)
            throw new SPSException(SPSException.invalid_param_code, "task", taskID);
        
        return task;
    }
    
    
    protected void handleRequest(GetStatusRequest request) throws Exception
    {
        ITask task = findTask(request.getTaskID());
        StatusReport status = task.getStatusReport();
        
        // security check
        String procID = task.getRequest().getProcedureID();
        securityHandler.checkPermission(getOfferingID(procID), securityHandler.sps_read_task);
        
        GetStatusResponse gsResponse = new GetStatusResponse();
        gsResponse.setVersion("2.0.0");
        gsResponse.getReportList().add(status);
        
        sendResponse(request, gsResponse);
    }
    
    
    protected GetFeasibilityResponse handleRequest(GetFeasibilityRequest request) throws Exception
    {               
        /*GetFeasibilityResponse gfResponse = new GetFeasibilityResponse();
        
        // create task in DB
        ITask newTask = taskDB.createNewTask(request);
        String studyId = newTask.getID();
        
        // launch feasibility study
        //FeasibilityResult result = doFeasibilityStudy(request);
        String sensorId = request.getSensorID();
        
        // create response
        GetFeasibilityResponse gfResponse = new GetFeasibilityResponse();
        gfResponse.setVersion("2.0.0");
        FeasibilityReport report = gfResponse.getReport();
        report.setTitle("Automatic Feasibility Results");
        report.setTaskID(studyId);
        report.setSensorID(sensorId);
                
        if (!isFeasible(result))
        {
            report.setRequestStatus(RequestStatus.Rejected);
        }
        else
        {
            report.setRequestStatus(RequestStatus.Accepted);
            report.setPercentCompletion(1.0f);            
        }
        
        report.touch();
        taskDB.updateTaskStatus(report);
        
        return gfResponse;*/  
        throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    
    
    protected void handleRequest(SubmitRequest request) throws Exception
    {
        // retrieve connector instance
        String procID = request.getProcedureID();
        ISPSConnector conn = getConnector(procID);
        
        // security check
        securityHandler.checkPermission(getOfferingID(procID), securityHandler.sps_task_submit);
        
        // validate task parameters
        request.validate();
        
        // create task in DB
        ITask newTask = taskDB.createNewTask(request);
        final String taskID = newTask.getID();
        
        // send command through connector
        DataBlockList dataBlockList = (DataBlockList)request.getParameters().getData();
        Iterator<DataBlock> it = dataBlockList.blockIterator();
        while (it.hasNext())
            conn.sendSubmitData(newTask, it.next());        
        
        // add report and send response
        SubmitResponse sResponse = new SubmitResponse();
        sResponse.setVersion("2.0");
        ITask task = findTask(taskID);
        task.getStatusReport().setRequestStatus(RequestStatus.Accepted);
        task.getStatusReport().setTaskStatus(TaskStatus.Completed);
        task.getStatusReport().touch();
        sResponse.setReport(task.getStatusReport());
        
        sendResponse(request, sResponse);
    }
    

    protected void handleRequest(UpdateRequest request) throws Exception
    {
        throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    
    
    protected void handleRequest(CancelRequest request) throws Exception
    {
        throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    

    protected void handleRequest(ReserveRequest request) throws Exception
    {
        throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    
    
    protected void handleRequest(ConfirmRequest request) throws Exception
    {
        throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    
    
    protected void handleRequest(DescribeResultAccessRequest request) throws Exception
    {
        /*ITask task = findTask(request.getTaskID());
        
        DescribeResultAccessResponse resp = new DescribeResultAccessResponse();     
        StatusReport status = task.getStatusReport();
        
        // TODO DescribeResultAccess
        
        return resp;*/
        throw new SPSException(SPSException.unsupported_op_code, request.getOperation());
    }
    
    
    //////////////////////////////
    // Transactional Operations //
    //////////////////////////////    
    
    protected void handleRequest(InsertSensorRequest request) throws Exception
    {
        try
        {
            checkTransactionalSupport(request);
            
            // security check
            securityHandler.checkPermission(securityHandler.sps_insert_sensor);
            
            // check query parameters
            OWSExceptionReport report = new OWSExceptionReport();
            TransactionUtils.checkSensorML(request.getProcedureDescription(), report);
            report.process();
                        
            // get sensor UID
            String sensorUID = request.getProcedureDescription().getUniqueIdentifier();
            log.info("Registering new sensor " + sensorUID);
            
            // offering name is derived from sensor UID
            String offeringID = sensorUID + "-sps";
            
            ///////////////////////////////////////////////////////////////////////////////////////
            // we configure things step by step so we can fix config if it was partially altered //
            ///////////////////////////////////////////////////////////////////////////////////////
            HashSet<ModuleConfig> configSaveList = new HashSet<ModuleConfig>();
            ModuleRegistry moduleReg = SensorHub.getInstance().getModuleRegistry();
            
            // create new virtual sensor module if needed            
            IModule<?> sensorModule = moduleReg.getLoadedModuleById(sensorUID);
            if (sensorModule == null)
            {
                sensorModule = TransactionUtils.createSensorModule(sensorUID, request.getProcedureDescription());
                configSaveList.add(sensorModule.getConfiguration());
            }            
            // else simply update description
            else
                TransactionUtils.updateSensorDescription(sensorModule, request.getProcedureDescription());
            
            // add new connector if needed
            ISPSConnector connector = connectors.get(sensorUID);
            if (connector == null)
            {
                // generate new provider config
                SensorConnectorConfig connectorConfig = new SensorConnectorConfig();
                connectorConfig.enabled = true;
                connectorConfig.sensorID = sensorUID;
                connectorConfig.offeringID = offeringID;
                config.connectors.replaceOrAdd(connectorConfig);
                
                // instantiate and register connector
                connector = connectorConfig.getConnector(this);
                connectors.put(sensorUID, connector);
                
                // add new permissions for this offering
                securityHandler.addOfferingPermissions(offeringID);
                
                configSaveList.add(config);
            }
            
            // save module configs so we don't loose anything on restart
            moduleReg.saveConfiguration(configSaveList.toArray(new ModuleConfig[0]));
            
            // update capabilities
            showConnectorCaps(connector);
            
            // build and send response
            InsertSensorResponse resp = new InsertSensorResponse();
            resp.setAssignedOffering(offeringID);
            resp.setAssignedProcedureId(sensorUID);
            sendResponse(request, resp);
        }
        finally
        {
            
        }
    }
    
    
    protected void handleRequest(InsertTaskingTemplateRequest request) throws Exception
    {
        try
        {
            checkTransactionalSupport(request);
            
            // retrieve connector instance
            String procID = request.getProcedureID();
            ISPSTransactionalConnector connector = getTransactionalConnector(procID);
                        
            // security check
            securityHandler.checkPermission(getOfferingID(procID), securityHandler.sps_connect_tasking);
                        
            // generate template ID
            String templateID = connector.newTaskingTemplate(request.getTaskingParameters(), request.getEncoding());
            
            // only continue of template was not already registered
            if (!templateToProcedureMap.containsKey(templateID))
            {
                try
                {
                    capabilitiesLock.writeLock().lock();
                    templateToProcedureMap.put(templateID, procID);
                
                    // re-generate capabilities
                    SPSOfferingCapabilities newCaps = connector.generateCapabilities();
                    int oldIndex = 0;
                    for (SPSOfferingCapabilities offCaps: capabilities.getLayers())
                    {
                        if (offCaps.getMainProcedure().equals(procID))
                            break; 
                        oldIndex++;
                    }
                    capabilities.getLayers().set(oldIndex, newCaps);
                    offeringCaps.put(procID, newCaps);
                }
                finally
                {
                    capabilitiesLock.writeLock().unlock();
                }
            }
            
            // build and send response
            InsertTaskingTemplateResponse resp = new InsertTaskingTemplateResponse();
            resp.setAcceptedTemplateId(templateID);
            sendResponse(request, resp);
        }
        finally
        {
            
        }
    }
    
    
    protected void handleRequest(ConnectTaskingRequest request) throws Exception
    {
        try
        {
            checkTransactionalSupport(request);
            
            // retrieve connector using template id
            String templateID = request.getTemplateId();
            String procID = getProcedureID(templateID);
            ISPSTransactionalConnector conn = getTransactionalConnector(procID);
            
            // security check
            securityHandler.checkPermission(getOfferingID(procID), securityHandler.sps_connect_tasking);
                        
            // prepare writer for selected template
            Template template = conn.getTemplate(templateID);
            final DataStreamWriter writer = SWEHelper.createDataWriter(template.encoding);
            writer.setDataComponents(template.component);
            
            // handle cases of websocket and persistent HTTP
            if (isWebSocketRequest(request))
            {
                SPSWebSocketOut ws = new SPSWebSocketOut(writer, log);
                conn.registerCallback(templateID, ws);
                acceptWebSocket(request, ws);
            }
            else
            {
                final AsyncContext aCtx = request.getHttpRequest().startAsync(request.getHttpRequest(), request.getHttpResponse());
                writer.setOutput(new BufferedOutputStream(request.getResponseStream()));
                
                conn.registerCallback(templateID, new ITaskingCallback() {
                    public void onCommandReceived(DataBlock cmdData)
                    {
                        try
                        {
                            writer.write(cmdData);
                            writer.flush();
                        }
                        catch (IOException e)
                        {
                            log.error("Error while sending command message to connected device. Closing stream.");
                            onClose();
                        }
                    }

                    public void onClose()
                    {
                        try
                        {
                            writer.close();
                            aCtx.complete();
                        }
                        catch (IOException e)
                        {
                        }
                    }
                });
            }
        }
        finally
        {
            
        }
    }
    
    
    protected ISPSConnector getConnector(String procedureID) throws Exception
    {
        try
        {
            capabilitiesLock.readLock().lock();
            ISPSConnector connector = connectors.get(procedureID);
            
            if (connector == null)
                throw new SPSException(SPSException.invalid_param_code, "procedure", procedureID);
            
            return connector;
        }
        finally
        {            
            capabilitiesLock.readLock().unlock();
        }
    }
    
    
    protected ISPSTransactionalConnector getTransactionalConnector(String procedureID) throws Exception
    {
        ISPSConnector connector = getConnector(procedureID);
        
        if (!(connector instanceof ISPSTransactionalConnector))
           throw new SPSException(SPSException.invalid_param_code, "procedure", procedureID, "Transactional operations are not supported for procedure " + procedureID);
                        
        return (ISPSTransactionalConnector)connector;
    }
    
    
    protected final String getOfferingID(String procedureID)
    {
        return offeringCaps.get(procedureID).getIdentifier();
    }
    
    
    protected String getProcedureID(String templateID) throws Exception
    {
        String procID = templateToProcedureMap.get(templateID);
        if (procID == null)
            throw new SPSException(SPSException.invalid_param_code, "template", templateID, "Invalid template ID");
        
        return procID;
    }
    
    
    protected void checkQueryProcedureFormat(String procedureID, String format, OWSExceptionReport report) throws SPSException
    {
        // ok if default format can be used
        if (format == null)
            return;
        
        SPSOfferingCapabilities offering = this.offeringCaps.get(procedureID);
        if (!offering.getProcedureFormats().contains(format))
            report.add(new SPSException(SPSException.invalid_param_code, "procedureDescriptionFormat", format, "Procedure description format " + format + " is not available for procedure " + procedureID));
    }
    
    
    protected void checkTransactionalSupport(OWSRequest request) throws Exception
    {
        if (!config.enableTransactional)
            throw new SPSException(SPSException.invalid_param_code, "request", request.getOperation(), request.getOperation() + " operation is not supported on this endpoint"); 
    }
    
    
    /*
     * Check if request is through websocket protocol
     */
    protected boolean isWebSocketRequest(OWSRequest request)
    {
        return request.getExtensions().containsKey(EXT_WS);
    }


    @Override
    protected String getServiceType()
    {
        return SPSUtils.SPS;
    }


    @Override
    protected String getDefaultVersion()
    {
        return "2.0";
    }
}
