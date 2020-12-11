/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.

Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.

******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import net.opengis.fes.v20.Conformance;
import net.opengis.fes.v20.FilterCapabilities;
import net.opengis.fes.v20.SpatialCapabilities;
import net.opengis.fes.v20.SpatialOperator;
import net.opengis.fes.v20.SpatialOperatorName;
import net.opengis.fes.v20.TemporalCapabilities;
import net.opengis.fes.v20.TemporalOperator;
import net.opengis.fes.v20.TemporalOperatorName;
import net.opengis.fes.v20.impl.FESFactory;
import net.opengis.swe.v20.BinaryBlock;
import net.opengis.swe.v20.BinaryEncoding;
import net.opengis.swe.v20.BinaryMember;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;
import org.eclipse.jetty.websocket.api.WebSocketBehavior;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.api.database.IProcedureObsDatabase;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.obs.DataStreamInfo;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.procedure.ProcedureId;
import org.sensorhub.api.procedure.ProcedureWrapper;
import org.sensorhub.api.security.ISecurityManager;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.service.HttpServer;
import org.sensorhub.impl.service.ogc.OGCServiceConfig.CapabilitiesInfo;
import org.sensorhub.impl.service.swe.RecordTemplate;
import org.sensorhub.impl.service.swe.TransactionUtils;
import org.sensorhub.utils.DataComponentChecks;
import org.sensorhub.utils.SWEDataUtils;
import org.slf4j.Logger;
import org.vast.ogc.gml.GeoJsonBindings;
import org.vast.ogc.om.IObservation;
import org.vast.ogc.om.SamplingPoint;
import org.vast.ows.GetCapabilitiesRequest;
import org.vast.ows.OWSException;
import org.vast.ows.OWSExceptionReport;
import org.vast.ows.OWSRequest;
import org.vast.ows.OWSUtils;
import org.vast.ows.sos.*;
import org.vast.ows.swe.DeleteSensorRequest;
import org.vast.ows.swe.DeleteSensorResponse;
import org.vast.ows.swe.DescribeSensorRequest;
import org.vast.ows.swe.SWESOfferingCapabilities;
import org.vast.ows.swe.UpdateSensorRequest;
import org.vast.ows.swe.UpdateSensorResponse;
import org.vast.util.TimeExtent;
import com.google.common.base.Strings;


/**
 * <p>
 * Extension of SOSServlet deployed as a SensorHub service
 * </p>
 *
 * @author Alex Robin
 * @since Sep 7, 2013
 */
@SuppressWarnings("serial")
public class SOSServlet extends org.vast.ows.sos.SOSServlet
{
    static final String INVALID_RESPONSE_FORMAT = "Unsupported response format: ";
    static final String INVALID_WS_REQ_MSG = "Invalid Websocket request: ";
    static final long GET_CAPS_MIN_REFRESH_PERIOD = 1000; // 1s
    static final String DEFAULT_PROVIDER_KEY = "%%%_DEFAULT_";

    final transient SOSService service;
    final transient SOSServiceConfig config;
    final transient SOSSecurity securityHandler;
    final transient SOSServiceCapabilities capabilities = new SOSServiceCapabilities();
    final transient NavigableMap<String, SOSProviderConfig> providerConfigs;

    final IProcedureObsDatabase readDatabase;
    final IProcedureObsDatabase writeDatabase;

    final transient Map<String, SOSCustomFormatConfig> customFormats = new HashMap<>();
    WebSocketServletFactory wsFactory;
    
    AtomicLong lastGetCapsRequest = new AtomicLong();


    protected SOSServlet(SOSService service, SOSSecurity securityHandler, Logger log) throws SensorHubException
    {
        super(log);

        this.service = service;
        this.config = service.getConfiguration();
        this.securityHandler = securityHandler;

        this.readDatabase = service.readDatabase;
        this.writeDatabase = service.writeDatabase;
        
        this.providerConfigs = new TreeMap<>();
        for (var config: service.getConfiguration().customDataProviders)
            providerConfigs.put(config.procedureUID, config);

        generateCapabilities();
    }


    @Override
    public void init(ServletConfig config) throws ServletException
    {
        super.init(config);

        // create websocket factory
        try
        {
            WebSocketPolicy wsPolicy = new WebSocketPolicy(WebSocketBehavior.SERVER);
            wsFactory = WebSocketServletFactory.Loader.load(getServletContext(), wsPolicy);
            wsFactory.start();
        }
        catch (Exception e)
        {
            throw new ServletException("Cannot initialize websocket factory", e);
        }
    }


    @Override
    public void destroy()
    {
        stop();

        // destroy websocket factory
        try
        {
            wsFactory.stop();
        }
        catch (Exception e)
        {
            log.error("Cannot stop websocket factory", e);
        }
    }


    protected void stop()
    {

    }


    /**
     * Generates the SOSServiceCapabilities object with info from data source
     */
    protected void generateCapabilities() throws SensorHubException
    {
        customFormats.clear();

        // get main capabilities info from config
        CapabilitiesInfo serviceInfo = config.ogcCapabilitiesInfo;
        capabilities.getSupportedVersions().add(DEFAULT_VERSION);
        capabilities.getIdentification().setTitle(serviceInfo.title);
        capabilities.getIdentification().setDescription(serviceInfo.description);
        capabilities.setFees(serviceInfo.fees);
        capabilities.setAccessConstraints(serviceInfo.accessConstraints);
        capabilities.setServiceProvider(serviceInfo.serviceProvider);

        // supported operations and extensions
        String endpoint = config.getPublicEndpoint();
        capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_RESULT_RETRIEVAL);
        capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_OMXML);
        capabilities.getGetServers().put("GetCapabilities", endpoint);
        capabilities.getGetServers().put("DescribeSensor", endpoint);
        capabilities.getGetServers().put("GetFeatureOfInterest", endpoint);
        capabilities.getGetServers().put("GetObservation", endpoint);
        capabilities.getGetServers().put("GetResult", endpoint);
        capabilities.getGetServers().put("GetResultTemplate", endpoint);
        capabilities.getPostServers().putAll(capabilities.getGetServers());

        if (config.enableTransactional)
        {
            capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_SENSOR_INSERTION);
            capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_SENSOR_DELETION);
            capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_OBS_INSERTION);
            capabilities.getProfiles().add(SOSServiceCapabilities.PROFILE_RESULT_INSERTION);
            capabilities.getPostServers().put("InsertSensor", endpoint);
            capabilities.getPostServers().put("DeleteSensor", endpoint);
            capabilities.getPostServers().put("InsertObservation", endpoint);
            capabilities.getPostServers().put("InsertResultTemplate", endpoint);
            capabilities.getPostServers().put("InsertResult", endpoint);
            capabilities.getGetServers().put("InsertResult", endpoint);

            // insertion capabilities
            SOSInsertionCapabilities insertCaps = new SOSInsertionCapabilities();
            insertCaps.getProcedureFormats().add(SWESOfferingCapabilities.FORMAT_SML2);
            insertCaps.getFoiTypes().add(SamplingPoint.TYPE);
            insertCaps.getObservationTypes().add(IObservation.OBS_TYPE_SCALAR);
            insertCaps.getObservationTypes().add(IObservation.OBS_TYPE_RECORD);
            insertCaps.getObservationTypes().add(IObservation.OBS_TYPE_ARRAY);
            insertCaps.getSupportedEncodings().add(SOSServiceCapabilities.SWE_ENCODING_TEXT);
            insertCaps.getSupportedEncodings().add(SOSServiceCapabilities.SWE_ENCODING_BINARY);
            capabilities.setInsertionCapabilities(insertCaps);
        }

        // filter capabilities
        FESFactory fac = new FESFactory();
        FilterCapabilities filterCaps = fac.newFilterCapabilities();
        capabilities.setFilterCapabilities(filterCaps);

        // conformance
        Conformance filterConform = filterCaps.getConformance();
        filterConform.addConstraint(fac.newConstraint("ImplementsQuery", Boolean.TRUE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsAdHocQuery", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsFunctions", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsResourceld", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsMinStandardFilter", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsStandardFilter", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsMinSpatialFilter", Boolean.TRUE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsSpatialFilter", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsMinTemporalFilter", Boolean.TRUE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsTemporalFilter", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsVersionNav", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsSorting", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsExtendedOperators", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsMinimumXPath", Boolean.FALSE.toString()));
        filterConform.addConstraint(fac.newConstraint("ImplementsSchemaElementFunc", Boolean.FALSE.toString()));

        // supported temporal filters
        TemporalCapabilities timeFilterCaps = fac.newTemporalCapabilities();
        timeFilterCaps.getTemporalOperands().add(new QName(null, "TimeInstant", "gml"));
        timeFilterCaps.getTemporalOperands().add(new QName(null, "TimePeriod", "gml"));
        TemporalOperator timeOp = fac.newTemporalOperator();
        timeOp.setName(TemporalOperatorName.DURING);
        timeFilterCaps.getTemporalOperators().add(timeOp);
        filterCaps.setTemporalCapabilities(timeFilterCaps);

        // supported spatial filters
        SpatialCapabilities spatialFilterCaps = fac.newSpatialCapabilities();
        spatialFilterCaps.getGeometryOperands().add(new QName(null, "Envelope", "gml"));
        SpatialOperator spatialOp = fac.newSpatialOperator();
        spatialOp.setName(SpatialOperatorName.BBOX);
        spatialFilterCaps.getSpatialOperators().add(spatialOp);
        filterCaps.setSpatialCapabilities(spatialFilterCaps);

        // build map of custom format serializers
        for (SOSCustomFormatConfig format: config.customFormats)
            customFormats.put(format.mimeType, format);
        
        // update offerings
        updateCapabilities();
    }


    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
    {
        // set current authentified user
        String userID = ISecurityManager.ANONYMOUS_USER;
        if (req.getRemoteUser() != null)
            userID = req.getRemoteUser();

        try
        {
            // check if we have an upgrade request for websockets
            if (wsFactory.isUpgradeRequest(req, resp))
            {
                // parse request
                try
                {
                    OWSRequest owsReq = this.parseRequest(req, resp, false);

                    if (owsReq != null)
                    {
                        owsReq.getExtensions().put(SOSProviderUtils.EXT_WS, true);

                        if (owsReq instanceof GetResultRequest)
                        {
                            acceptWebSocket(owsReq, new SOSWebSocketOut(this, owsReq, userID, log));
                        }
                        else if (owsReq instanceof InsertResultRequest)
                        {
                            this.handleRequest(owsReq);
                        }
                        else
                            throw new ServletException(INVALID_WS_REQ_MSG + owsReq.getOperation() + " is not supported via this protocol");
                    }
                }
                catch (Exception e)
                {
                    String errorMsg = "Error while processing Websocket request";
                    resp.sendError(400, errorMsg);
                    log.trace(errorMsg, e);
                }

                return;
            }

            // otherwise process as classical HTTP request
            securityHandler.setCurrentUser(userID);
            super.service(req, resp);
        }
        finally
        {
            securityHandler.clearCurrentUser();
        }
    }


    protected void acceptWebSocket(final OWSRequest owsReq, final WebSocketListener socket) throws IOException
    {
        wsFactory.acceptWebSocket(new WebSocketCreator() {
            @Override
            public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp)
            {
                return socket;
            }
        }, owsReq.getHttpRequest(), owsReq.getHttpResponse());
    }


    @Override
    protected void handleRequest(GetCapabilitiesRequest request) throws IOException, OWSException
    {
        // check that version 2.0.0 is supported by client
        if (!request.getAcceptedVersions().isEmpty())
        {
            if (!request.getAcceptedVersions().contains(DEFAULT_VERSION))
                throw new SOSException(SOSException.version_nego_failed_code, "AcceptVersions", null,
                        "Only version " + DEFAULT_VERSION + " is supported by this server");
        }

        // set selected version
        request.setVersion(DEFAULT_VERSION);

        // security check
        securityHandler.checkPermission(securityHandler.sos_read_caps);
        
        // update operation URLs dynamically if base URL not set in config
        if (Strings.isNullOrEmpty(HttpServer.getInstance().getConfiguration().proxyBaseUrl))
        {
            String endpointUrl = request.getHttpRequest().getRequestURL().toString();
            capabilities.updateAllEndpointUrls(endpointUrl);
        }
        
        // send async response
        var asyncCtx = request.getHttpRequest().startAsync();
        CompletableFuture.runAsync(() -> {
            
            try
            {
                // fence updater to throttle at max refresh rate
                var now = System.currentTimeMillis();
                while (true)
                {
                    long local = lastGetCapsRequest.get();
                    if (now >= local + GET_CAPS_MIN_REFRESH_PERIOD)
                    {
                        if (lastGetCapsRequest.compareAndSet(local, now))
                        {
                            updateCapabilities();
                            break;
                        }
                    }
                    else
                        break;
                }            
            
                var os = asyncCtx.getResponse().getOutputStream();
                owsUtils.writeXMLResponse(os, capabilities, request.getVersion(), request.getSoapVersion());
                os.flush();
                asyncCtx.complete();
            }
            catch (Exception e)
            {
                handleError(
                    (HttpServletRequest)asyncCtx.getRequest(),
                    (HttpServletResponse)asyncCtx.getResponse(),
                    request, e);
            }
        }, service.getThreadPool());
    }
    
    
    protected SOSServiceCapabilities updateCapabilities()
    {
        new CapabilitiesUpdater().updateOfferings(capabilities, readDatabase, providerConfigs);
        getLogger().debug("Updating capabilities");
        return capabilities;
    }   


    @Override
    protected void handleRequest(DescribeSensorRequest request) throws IOException, OWSException
    {
        // security check
        securityHandler.checkPermission(securityHandler.sos_read_sensor);
        
        // check query parameters
        String procUID = request.getProcedureID();
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedure(procUID, report);
        checkQueryTime(request.getTime(), report);
        
        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncProcedureSerializer serializer = null;
        if (format == null || isXmlMimeType(format) || SWESOfferingCapabilities.FORMAT_SML2.equals(format))
            serializer = new ProcedureSerializerXml();
        else if (SWESOfferingCapabilities.FORMAT_SML2_JSON.equals(format) ||
            OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new ProcedureSerializerJson();
        else
            report.add(new SOSException(SOSException.invalid_param_code, "procedureDescriptionFormat",
                format, INVALID_RESPONSE_FORMAT + format));
        report.process();
        
        // create data provider
        var dataProvider = getDataProvider(procUID, request);

        // start async response
        AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        serializer.init(this, asyncCtx, request);
        dataProvider.getProcedureDescriptions(request, serializer);
    }


    @Override
    protected void handleRequest(GetObservationRequest request) throws IOException, OWSException
    {
        // security check
        securityHandler.checkPermission(securityHandler.sos_read_obs);

        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryOffering(request.getOffering(), report);
        
        // build procedure UID set
        Set<String> selectedProcedures = new HashSet<>();
        selectedProcedures.addAll(request.getProcedures());
        if (selectedProcedures.isEmpty())
            selectedProcedures.addAll(request.getOfferings());
        else if (!request.getOfferings().isEmpty())
            selectedProcedures.retainAll(request.getOfferings());        
        checkQueryProcedures(selectedProcedures, report);
        
        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncObsSerializer serializer = null;
        if (format == null || isXmlMimeType(format))
            serializer = new ObsSerializerXml();
        else if (OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new ObsSerializerJson();
        else
            report.add(new SOSException(SOSException.invalid_param_code, "responseFormat",
                format, INVALID_RESPONSE_FORMAT + format));
        report.process();
                
        // get all selected providers
        var dataProviders = getDataProviders(selectedProcedures, request);
        
        // start async response
        final AsyncContext asyncCtx = request.getHttpRequest().startAsync();
                
        // retrieve and serialize obs collection from each provider in sequence
        serializer.init(this, asyncCtx, request);
        request.getProcedures().addAll(selectedProcedures);
        new GetObsMultiProviderSubscriber(dataProviders, request, serializer).start();
    }


    @Override
    protected void handleRequest(GetResultTemplateRequest request) throws IOException, OWSException
    {
        // security check
        securityHandler.checkPermission(securityHandler.sos_read_obs);
        
        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryOffering(request.getOffering(), report);

        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncResultTemplateSerializer serializer;
        if (format == null || isXmlMimeType(format))
            serializer = new ResultTemplateSerializerXml();
        else if (OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new ResultTemplateSerializerJson();
        else
        {
            serializer = null;
            report.add(new SOSException(SOSException.invalid_param_code, "responseFormat",
                format, INVALID_RESPONSE_FORMAT + format));
        }
        report.process(); // will throw exception it report contains one
        
        // get data provider
        var procUID = getProcedureUID(request.getOffering());
        ISOSAsyncDataProvider dataProvider = getDataProvider(procUID, request);
        
        // start async response
        final AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        
        // fetch result template
        serializer.init(this, asyncCtx, request);
        dataProvider.getResultTemplate(request).thenAccept(resultTemplate -> {
            try
            {
                // create simple subscription
                serializer.onSubscribe(new Subscription() {
                    boolean done = false;
                    public void request(long n)
                    {
                        if (!done)
                        {
                            // send back result template synchronously
                            done = true;
                            serializer.onNext(resultTemplate);
                            serializer.onComplete();                            
                        }
                    }

                    @Override
                    public void cancel()
                    {                        
                    }
                });
            }
            catch (Exception e)
            {
                throw new CompletionException(e);
            }
        }).exceptionally(ex -> {
            handleError(
                (HttpServletRequest)asyncCtx.getRequest(),
                (HttpServletResponse)asyncCtx.getResponse(),
                request, ex);
            return null;
        });
    }


    @Override
    protected void handleRequest(GetResultRequest request) throws IOException, OWSException
    {
        // security check
        securityHandler.checkPermission(securityHandler.sos_read_obs);
        
        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryOffering(request.getOffering(), report);
        checkQueryTime(request.getTime(), report);
        report.process();

        // create data provider
        String procUID = getProcedureUID(request.getOffering());
        ISOSAsyncDataProvider dataProvider = getDataProvider(procUID, request);
        
        // create GetResultTemplate request
        var grt = new GetResultTemplateRequest();
        grt.setOffering(request.getOffering());
        grt.setObservables(request.getObservables());
        
        // start async or websocket response
        final AsyncContext asyncCtx;
        if (!SOSProviderUtils.isWebSocketRequest(request))
        {
            asyncCtx = request.getHttpRequest().startAsync();
            
            // disable async timeout to allow long-lived streaming connections 
            if (dataProvider instanceof StreamingDataProvider ||
                SOSProviderUtils.isReplayRequest(request))
                asyncCtx.setTimeout(0);
        }
        else
            asyncCtx = null;
        
        // fetch result template, then process record stream asychronously
        dataProvider.getResultTemplate(grt).thenAccept(resultTemplate -> {
            try
            {
                // choose serializer according to output format
                String format = request.getFormat();
                ISOSAsyncResultSerializer serializer;
                if (isXmlMimeType(format))
                    serializer = new ResultSerializerXml();
                else if (OWSUtils.TEXT_MIME_TYPE.equals(format))
                    serializer = new ResultSerializerText();
                else if (OWSUtils.JSON_MIME_TYPE.equals(format))
                    serializer = new ResultSerializerJson();
                else if (OWSUtils.BINARY_MIME_TYPE.equals(format))
                    serializer = new ResultSerializerBinary();
                else
                    serializer = getCustomFormatSerializer(request, resultTemplate);
                
                // if no specific format was requested or auto-detected, use default
                if (serializer == null)
                    serializer = getDefaultSerializer(request, resultTemplate);

                // subscribe and stream results asynchronously
                serializer.init(this, asyncCtx, request, resultTemplate);
                dataProvider.getResults(request, serializer);
            }
            catch (Exception e)
            {
                throw new CompletionException(e);
            }
        }).exceptionally(ex -> {
            handleError(
                (HttpServletRequest)asyncCtx.getRequest(),
                (HttpServletResponse)asyncCtx.getResponse(),
                request, ex);
            return null;
        });;
    }


    @Override
    protected void handleRequest(final GetFeatureOfInterestRequest request) throws IOException, OWSException
    {
        // security check
        securityHandler.checkPermission(securityHandler.sos_read_foi);
        
        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedures(request.getProcedures(), report);
        
        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncFeatureSerializer serializer = null;
        if (format == null || isXmlMimeType(format))
            serializer = new FeatureSerializerGml();
        else if (GeoJsonBindings.MIME_TYPE.equals(format) || OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new FeatureSerializerGeoJson();
        else
            report.add(new SOSException(SOSException.invalid_param_code, "responseFormat",
                format, INVALID_RESPONSE_FORMAT + format));
        report.process();
        
        // create data provider
        ISOSAsyncDataProvider dataProvider = getDefaultDataProvider(request);
        
        // start async response
        AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        serializer.init(this, asyncCtx, request);
        dataProvider.getFeaturesOfInterest(request, serializer);
    }


    @Override
    protected void handleRequest(InsertSensorRequest request) throws IOException, OWSException
    {
        checkTransactionalSupport(request);
        
        // security check
        securityHandler.checkPermission(securityHandler.sos_insert_sensor);

        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        TransactionUtils.checkSensorML(request.getProcedureDescription(), report);
        report.process();

        // get sensor UID
        String procUID = request.getProcedureDescription().getUniqueIdentifier();
        getLogger().info("Registering new sensor {}", procUID);

        // add description to DB
        try
        {
            writeDatabase.getProcedureStore().add(request.getProcedureDescription());
        }
        catch (DataStoreException e)
        {
            throw new SOSException(SOSException.invalid_param_code, "procedureDescription", null,
                "Procedure " + procUID + " is already registered on this server");
        }

        // build and send response
        InsertSensorResponse resp = new InsertSensorResponse();
        resp.setAssignedOffering(getOfferingID(procUID));
        resp.setAssignedProcedureId(procUID);
        sendResponse(request, resp);
    }


    @Override
    protected void handleRequest(DeleteSensorRequest request) throws IOException, OWSException
    {
        checkTransactionalSupport(request);

        // security check
        securityHandler.checkPermission(securityHandler.sos_delete_sensor);

        // check query parameters
        String procUID = request.getProcedureId();
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedure(procUID, report);
        report.process();
        
        // delete complete procedure history + all datastreams and obs from DB
        try
        {
            writeDatabase.executeTransaction(() -> {
                
                writeDatabase.getDataStreamStore().removeEntries(new DataStreamFilter.Builder()
                    .withProcedures().withUniqueIDs(procUID).done()
                    .build());
                
                writeDatabase.getProcedureStore().remove(procUID);
                
                return null;
            });            
        }
        catch (Exception e)
        {
            throw new IOException("Cannot delete procedure " + procUID, e);
        }

        // build and send response
        DeleteSensorResponse resp = new DeleteSensorResponse(SOSUtils.SOS);
        resp.setDeletedProcedure(procUID);
        sendResponse(request, resp);
    }


    @Override
    protected void handleRequest(UpdateSensorRequest request) throws IOException, OWSException
    {
        checkTransactionalSupport(request);

        // security check
        securityHandler.checkPermission(securityHandler.sos_update_sensor);
        
        // check query parameters
        String procUID = request.getProcedureId();
        var procDesc = request.getProcedureDescription();
        OWSExceptionReport report = new OWSExceptionReport();
        var fk = checkQueryProcedure(procUID, report);
        TransactionUtils.checkSensorML(procDesc, report);
        report.process();

        // version or replace description in DB
        var validTime = procDesc.getValidTime();
        try
        {
            procDesc.setUniqueIdentifier(procUID);
            if (validTime == null || validTime.begin().equals(fk.getValidStartTime()))
                writeDatabase.getProcedureStore().put(fk, new ProcedureWrapper(procDesc));
            else if (validTime.begin().isAfter(fk.getValidStartTime()))
                writeDatabase.getProcedureStore().add(request.getProcedureDescription());
            else
                throw new SOSException(SOSException.invalid_param_code, "procedureDescription", null,
                    "The procedure description's validity time period must start at the same time " + 
                    "or after the currently valid description");
        }
        catch (DataStoreException e)
        {
            throw new IOException("Cannot update procedure", e);
        }

        // build and send response
        UpdateSensorResponse resp = new UpdateSensorResponse(SOSUtils.SOS);
        resp.setUpdatedProcedure(procUID);
        sendResponse(request, resp);
    }


    @Override
    protected void handleRequest(InsertObservationRequest request) throws IOException, OWSException
    {
        checkTransactionalSupport(request);

        /*// retrieve proxy for selected offering
        ProcedureProxyImpl proxy = getProcedureProxyByOfferingID(request.getOffering());

        // security check
        securityHandler.checkPermission(proxy.getUniqueIdentifier(), securityHandler.sos_insert_obs);

        // TODO send new observation
        throw new SOSException(SOSException.invalid_request_code, null, null,
            "InsertObservation not supported yet. Please use InsertResult.");

        // build and send response
        //InsertObservationResponse resp = new InsertObservationResponse();
        //sendResponse(request, resp);*/
    }


    @Override
    protected void handleRequest(InsertResultTemplateRequest request) throws IOException, OWSException
    {
        checkTransactionalSupport(request);

        // security check
        securityHandler.checkPermission(securityHandler.sos_insert_obs);
        
        // check query parameters
        String procUID = getProcedureUID(request.getOffering());
        OWSExceptionReport report = new OWSExceptionReport();
        var procKey = checkQueryProcedure(procUID, report);
        report.process();
                
        var resultStruct = request.getResultStructure();
        var resultEncoding = request.getResultEncoding();
        
        // get procedure internal key
        var procID = getParentHub().getDatabaseRegistry().getLocalID(
            writeDatabase.getDatabaseNum(), procKey.getInternalID());
        
        // get existing datastreams of this procedure
        var outputs = writeDatabase.getDataStreamStore().selectEntries(new DataStreamFilter.Builder()
                .withProcedures(procID)
                .withCurrentVersion()
                .build())
            .collect(Collectors.toMap(
                dsEntry -> dsEntry.getValue().getOutputName(),
                dsEntry -> dsEntry));
        
        // use hash to check if a datastream with exact same description exists
        var sameOutput = findIdenticalDatastream(resultStruct, outputs.values());
        String outputName = null;
        
        // continue only if datastream is different from any previously registered datastream
        if (sameOutput == null)
        {
            var existingOutput = findCompatibleDatastream(resultStruct, outputs.values());
            
            // if output with same structure already exists, replace it
            if (existingOutput != null)
            {
                outputName = existingOutput.getValue().getOutputName();
                resultStruct.setName(outputName);
                
                var dsKey = existingOutput.getKey();
                writeDatabase.getDataStreamStore().put(dsKey, new DataStreamInfo.Builder()
                    .withProcedure(new ProcedureId(procID, procUID))
                    .withRecordDescription(resultStruct)
                    .withRecordEncoding(resultEncoding)
                    .build());
            }
            
            // else add it or version it
            else
            {
                // generate output name
                outputName = generateOutputName(resultStruct, outputs.size());
                resultStruct.setName(outputName);
                
                try
                {
                    writeDatabase.getDataStreamStore().add(new DataStreamInfo.Builder()
                        .withProcedure(new ProcedureId(procID, procUID))
                        .withRecordDescription(resultStruct)
                        .withRecordEncoding(resultEncoding)
                        .build());
                }
                catch (DataStoreException e)
                {
                    throw new IllegalStateException("Cannot add new datastream", e);
                }
            }
        }
        else
            outputName = sameOutput.getValue().getOutputName();
        
        // build and send response
        String templateID = generateTemplateID(procUID, outputName);
        InsertResultTemplateResponse resp = new InsertResultTemplateResponse();
        resp.setAcceptedTemplateId(templateID);
        sendResponse(request, resp);
    }
    
    
    protected Entry<DataStreamKey, IDataStreamInfo> findIdenticalDatastream(DataComponent resultStruct, Collection<Entry<DataStreamKey, IDataStreamInfo>> outputList)
    {
        var newHc = DataComponentChecks.getStructEqualsHashCode(resultStruct);
        for (var output: outputList)
        {
            var recordStruct = output.getValue().getRecordStructure();
            var oldHc = DataComponentChecks.getStructEqualsHashCode(recordStruct);
            if (newHc.equals(oldHc))
                return output;
        }
        
        return null;
    }
    
    
    protected Entry<DataStreamKey, IDataStreamInfo> findCompatibleDatastream(DataComponent resultStruct, Collection<Entry<DataStreamKey, IDataStreamInfo>> outputList)
    {
        var newHc = DataComponentChecks.getStructCompatibilityHashCode(resultStruct);
        for (var output: outputList)
        {
            var recordStruct = output.getValue().getRecordStructure();
            var oldHc = DataComponentChecks.getStructCompatibilityHashCode(recordStruct);
            if (newHc.equals(oldHc))
                return output;
        }
        
        return null;
    }
    
    
    protected String generateOutputName(DataComponent resultStructure, int numOutputs)
    {
        // use ID or label if provided in result template
        if (resultStructure.getId() != null)
            return SWEDataUtils.toNCName(resultStructure.getId());
        else if (resultStructure.getLabel() != null)
            return SWEDataUtils.toNCName(resultStructure.getLabel());
        
        // otherwise generate an output name with numeric index
        return String.format("output%02d", numOutputs+1);
    }
    
    
    protected final String generateTemplateID(String procUID, String outputName)
    {
        return procUID + '#' + outputName;
    }
    
    
    protected final String getOutputNameFromTemplateID(String templateID)
    {
        return templateID.substring(templateID.lastIndexOf('#')+1);
    }


    @Override
    protected void handleRequest(InsertResultRequest request) throws IOException, OWSException
    {
        checkTransactionalSupport(request);
        
        // security check
        securityHandler.checkPermission(securityHandler.sos_insert_obs);

        /*DataStreamParser parser = null;
        // retrieve consumer based on template id
        String templateID = request.getTemplateId();
        ProcedureProxyImpl proxy = getProcedureProxyByTemplateID(templateID);
        SensorDataConsumer consumer = new SensorDataConsumer(proxy);

        // security check
        securityHandler.checkPermission(proxy.getUniqueIdentifier(), securityHandler.sos_insert_obs);

        // get template info
        RecordTemplate template = consumer.getTemplate(templateID);
        DataComponent dataStructure = template.getDataStructure();
        DataEncoding encoding = template.getDataEncoding();

        try
        {
            InputStream resultStream;

            // select data source (either inline XML or in POST body for KVP)
            DataSource dataSrc = request.getResultDataSource();
            if (dataSrc instanceof DataSourceDOM) // inline XML
            {
                encoding = SWEHelper.ensureXmlCompatible(encoding);
                resultStream = dataSrc.getDataStream();
            }
            else // POST body
            {
                resultStream = new BufferedInputStream(request.getHttpRequest().getInputStream());
            }

            // create parser
            parser = SWEHelper.createDataParser(encoding);
            parser.setDataComponents(dataStructure);
            parser.setInput(resultStream);

            // if websocket, parse records in the callback
            if (isWebSocketRequest(request))
            {
                WebSocketListener socket = new SOSWebSocketIn(parser, consumer, templateID, log);
                this.acceptWebSocket(request, socket);
            }
            else
            {
                // parse each record and send it to consumer
                DataBlock nextBlock = null;
                while ((nextBlock = parser.parseNextBlock()) != null)
                    consumer.newResultRecord(templateID, nextBlock);

                // build and send response
                InsertResultResponse resp = new InsertResultResponse();
                sendResponse(request, resp);
            }
        }
        catch (ReaderException e)
        {
            throw new SOSException("Error in SWE encoded data", e);
        }
        finally
        {
            if (parser != null)
                parser.close();
        }*/
    }
    
    
    protected void checkQueryOffering(String offeringID, OWSExceptionReport report) throws SOSException
    {
        var procUID = getProcedureUID(offeringID);
        if (procUID == null || !readDatabase.getProcedureStore().contains(procUID))
            report.add(new SOSException(SOSException.invalid_param_code, "offering", offeringID, "Unknown offering: " + offeringID));
    }


    protected void checkQueryOfferings(Set<String> offerings, OWSExceptionReport report) throws SOSException
    {
        for (String offeringID: offerings)
            checkQueryOffering(offeringID, report);
    }


    protected FeatureKey checkQueryProcedure(String procUID, OWSExceptionReport report) throws SOSException
    {
        FeatureKey fk = null;
        if (procUID == null || (fk = readDatabase.getProcedureStore().getCurrentVersionKey(procUID)) == null)
            report.add(new SOSException(SOSException.invalid_param_code, "procedure", procUID, "Unknown procedure: " + procUID));
        return fk;
    }


    protected void checkQueryProcedures(Set<String> procedures, OWSExceptionReport report) throws SOSException
    {
        for (String procUID: procedures)
            checkQueryProcedure(procUID, report);
    }
    
    
    protected void checkQueryTime(TimeExtent requestTime, OWSExceptionReport report) throws SOSException
    {
        // reject null time period
        if (requestTime == null)
            return;
        
        // reject if startTime > stopTime
        if (requestTime.begin().isAfter(requestTime.end()))
            report.add(new SOSException("The requested period must begin before it ends"));            
    }


    protected ISOSAsyncDataProvider getDataProvider(String procUID, OWSRequest request) throws IOException, OWSException
    {
        try
        {
            SOSProviderConfig config = providerConfigs.get(procUID);

            // if no custom config, use default provider
            if (config == null)
                return getDefaultDataProvider(request);
            else
                return config.createProvider(service, request);
        }
        catch (SensorHubException e)
        {
            throw new IOException("Cannot get provider for procedure " + procUID, e);
        }
    }


    protected Map<String, ISOSAsyncDataProvider> getDataProviders(Set<String> procUIDs, OWSRequest request) throws IOException, OWSException
    {
        var providerMap = new LinkedHashMap<String, ISOSAsyncDataProvider>();
        
        for (String procUID: procUIDs)
        {
            try
            {
                if (providerConfigs.containsKey(procUID))
                {
                    SOSProviderConfig config = providerConfigs.get(procUID);
                    var customDataProvider = config.createProvider(service, request);
                    providerMap.put(procUID, customDataProvider);
                }
                
                else if (!providerMap.containsKey(DEFAULT_PROVIDER_KEY))
                    providerMap.put(DEFAULT_PROVIDER_KEY, getDefaultDataProvider(request));
            }
            catch (Exception e)
            {
                throw new IOException("Cannot get provider for procedure " + procUID, e);
            }
        }
        
        // if no offering or procedure was specified select all available
        // procedures from default provider
        if (procUIDs.isEmpty())
            providerMap.put(DEFAULT_PROVIDER_KEY, getDefaultDataProvider(request));
        
        return providerMap;
    }
    
    
    protected ISOSAsyncDataProvider getDefaultDataProvider(OWSRequest request) throws IOException, OWSException
    {
        try
        {
            return new ProcedureDataProviderConfig().createProvider(service, request);
        }
        catch (SensorHubException e)
        {
            throw new IOException("Cannot get default provider", e);
        }
    }


    protected void checkTransactionalSupport(OWSRequest request) throws SOSException
    {
        if (!config.enableTransactional || writeDatabase == null)
            throw new SOSException(SOSException.invalid_param_code, "request", request.getOperation(), request.getOperation() + " operation is not supported on this endpoint");
    }


    @Override
    protected String getDefaultVersion()
    {
        return DEFAULT_VERSION;
    }


    protected void addProducerID(DataComponent dataStruct, String foiUriPrefix)
    {
        /*SWEHelper fac = new SWEHelper();

        // use category component if prefix was detected
        // or text component otherwise
        DataComponent producerIdField;
        if (foiUriPrefix != null && foiUriPrefix.length() > 2)
            producerIdField = fac.newCategory(SWEConstants.DEF_PROCEDURE_ID, PROCEDURE_ID_LABEL, null, foiUriPrefix);
        else
            producerIdField = fac.newText(SWEConstants.DEF_PROCEDURE_ID, PROCEDURE_ID_LABEL, null);

        // insert FOI component in data record after time stamp
        String producerIdName = "procedureID";
        if (dataStruct instanceof DataRecord)
        {
            OgcPropertyList<DataComponent> fields = ((DataRecord) dataStruct).getFieldList();
            producerIdField.setName(producerIdName);
            if (!fields.isEmpty() && fields.get(0) instanceof Time)
                fields.add(1, producerIdField);
            else
                fields.add(0, producerIdField);
        }
        else
        {
            DataRecord rec = fac.newDataRecord();
            rec.addField(producerIdName, producerIdField);
            rec.addField("data", dataStruct);
        }*/
    }


    /*
     * Check if request comes from a compatible browser
     */
    protected boolean isRequestFromBrowser(OWSRequest request)
    {
        // don't do multipart with websockets
        HttpServletRequest httpRequest = request.getHttpRequest();
        if (httpRequest == null)
            return false;
        if (request.getHttpResponse() == null)
            return false;

        String userAgent = httpRequest.getHeader("User-Agent");
        if (userAgent == null)
            return false;

        if (userAgent.contains("Firefox") ||
            userAgent.contains("Chrome") ||
            userAgent.contains("Safari"))
            return true;

        return false;
    }
    
    
    protected boolean isXmlMimeType(String format)
    {
        return OWSUtils.XML_MIME_TYPE.equals(format) ||
               OWSUtils.XML_MIME_TYPE2.equals(format);
    }


    protected void startSoapEnvelope(OWSRequest request, XMLStreamWriter writer) throws XMLStreamException
    {
        String soapUri = request.getSoapVersion();
        if (soapUri != null)
        {
            writer.writeStartElement(SOAP_PREFIX, "Envelope", soapUri);
            writer.writeNamespace(SOAP_PREFIX, soapUri);
            writer.writeStartElement(SOAP_PREFIX, "Body", soapUri);
        }
    }


    protected void endSoapEnvelope(OWSRequest request, XMLStreamWriter writer) throws XMLStreamException
    {
        String soapUri = request.getSoapVersion();
        if (soapUri != null)
        {
            writer.writeEndElement();
            writer.writeEndElement();
        }
    }
    
    
    protected ISOSAsyncResultSerializer getDefaultSerializer(GetResultRequest request,  RecordTemplate resultTemplate) throws SOSException
    {
        // select serializer depending on encoding type
        if (resultTemplate.getDataEncoding() instanceof BinaryEncoding)
            return new ResultSerializerBinary();
        else
            return new ResultSerializerText();
    }


    protected ISOSAsyncResultSerializer getCustomFormatSerializer(GetResultRequest request,  RecordTemplate resultTemplate) throws SOSException
    {
        String format = request.getFormat();

        // auto select video format in some common cases
        if (format == null)
        {
            DataEncoding resultEncoding = resultTemplate.getDataEncoding();
            if (resultEncoding instanceof BinaryEncoding)
            {
                List<BinaryMember> mbrList = ((BinaryEncoding)resultEncoding).getMemberList();
                BinaryBlock videoFrameSpec = null;

                // try to find binary block encoding def in list
                for (BinaryMember spec: mbrList)
                {
                    if (spec instanceof BinaryBlock)
                    {
                        videoFrameSpec = (BinaryBlock)spec;
                        break;
                    }
                }

                if (videoFrameSpec != null)
                {
                    if (isRequestFromBrowser(request) && videoFrameSpec.getCompression().equals("H264"))
                        format = "video/mp4";

                    else if (isRequestFromBrowser(request) && videoFrameSpec.getCompression().equals("JPEG"))
                        format = "video/x-motion-jpeg";
                }
            }
            
            if (format == null)
                return null;
        }

        // try to find a matching implementation for selected format
        SOSCustomFormatConfig formatConfig;
        if (format != null && (formatConfig = customFormats.get(format)) != null)
        {
            try
            {
                ModuleRegistry moduleReg = service.getParentHub().getModuleRegistry();
                return (ISOSAsyncResultSerializer)moduleReg.loadClass(formatConfig.className);
            }
            catch (Exception e)
            {
                log.error("Error while initializing custom serializer for " + formatConfig.mimeType + " serializer", e);
            }
        }

        throw new SOSException(SOSException.invalid_param_code, "format",
            format, INVALID_RESPONSE_FORMAT + format);
    }


    public String getProcedureUID(String offeringID)
    {
        // for now, assume offerings have same URI as procedures
        return offeringID;
    }


    public String getOfferingID(String procedureUID)
    {
        // for now, assume offerings have same URI as procedures
        return procedureUID;
    }


    protected ISensorHub getParentHub()
    {
        return service.getParentHub();
    }


    protected Logger getLogger()
    {
        return log;
    }
}
