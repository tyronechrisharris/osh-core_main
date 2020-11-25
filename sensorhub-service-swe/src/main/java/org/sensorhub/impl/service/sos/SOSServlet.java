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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLOutputFactory;
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
import net.opengis.gml.v32.AbstractFeature;
import net.opengis.sensorml.v20.AbstractProcess;
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
import org.sensorhub.api.persistence.FoiFilter;
import org.sensorhub.api.persistence.IFoiFilter;
import org.sensorhub.api.security.ISecurityManager;
import org.sensorhub.api.service.ServiceException;
import org.sensorhub.impl.event.DelegatingSubscriber;
import org.sensorhub.impl.module.ModuleRegistry;
import org.sensorhub.impl.service.HttpServer;
import org.sensorhub.impl.service.ogc.OGCServiceConfig.CapabilitiesInfo;
import org.sensorhub.impl.service.swe.RecordTemplate;
import org.slf4j.Logger;
import org.vast.json.JsonStreamException;
import org.vast.ogc.OGCRegistry;
import org.vast.ogc.gml.GMLStaxBindings;
import org.vast.ogc.gml.GenericFeature;
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
import org.vast.ows.swe.DescribeSensorRequest;
import org.vast.ows.swe.SWESOfferingCapabilities;
import org.vast.ows.swe.UpdateSensorRequest;
import org.vast.sensorML.SMLStaxBindings;
import org.vast.sensorML.json.SMLJsonStreamWriter;
import org.vast.swe.SWEConstants;
import org.vast.swe.SWEStaxBindings;
import org.vast.swe.json.SWEJsonStreamWriter;
import org.vast.util.TimeExtent;
import org.vast.xml.IndentingXMLStreamWriter;
import org.vast.xml.XMLImplFinder;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.vividsolutions.jts.geom.Polygon;


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
    private static final String INVALID_WS_REQ_MSG = "Invalid Websocket request: ";
    private static final QName EXT_REPLAY = new QName("replayspeed"); // kvp params are always lower case
    private static final QName EXT_WS = new QName("websocket");
    private static final String PROCEDURE_ID_LABEL = "Procedure ID";
    private static final long GET_CAPS_MIN_REFRESH_PERIOD = 1000; // 1s

    final transient SOSService service;
    final transient SOSServiceConfig config;
    final transient SOSSecurity securityHandler;
    final transient SOSServiceCapabilities capabilities = new SOSServiceCapabilities();
    final transient NavigableMap<String, SOSProviderConfig> providerConfigs;

    final IProcedureObsDatabase readDatabase;
    final IProcedureObsDatabase writeDatabase;
    final transient Cache<String, String> templateToProcedureMap;

    final transient Map<String, ISOSAsyncResultSerializer> customFormats = new HashMap<>();
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
        
        this.templateToProcedureMap = CacheBuilder.newBuilder()
            .expireAfterAccess(config.templateTimeout, TimeUnit.SECONDS)
            .build();

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
        templateToProcedureMap.cleanUp();
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

        // preload custom format serializers
        ModuleRegistry moduleReg = service.getParentHub().getModuleRegistry();
        for (SOSCustomFormatConfig allowedFormat: config.customFormats)
        {
            /*try
            {
                ISOSAsyncResultSerializer serializer = (ISOSAsyncResultSerializer)moduleReg.loadClass(allowedFormat.className);
                customFormats.put(allowedFormat.mimeType, serializer);
            }
            catch (Exception e)
            {
                log.error("Error while initializing custom " + allowedFormat.mimeType + " serializer", e);
            }*/
        }
        
        // update offerings
        updateCapabilities();
    }


    /*
     * Retrieves SensorML object for the given procedure unique ID
     */
    protected AbstractProcess generateSensorML(String uri, TimeExtent timeExtent) throws ServiceException
    {
        /*try
        {
            ProcedureProxyImpl proxy = getProcedureProxyByOfferingID(uri);
            double time = Double.NaN;
            if (timeExtent != null)
                time = timeExtent.getBaseTime();
            return factory.generateSensorMLDescription(time);
        }
        catch (Exception e)
        {
            throw new ServiceException("Error while retrieving SensorML description for sensor " + uri, e);
        }*/
        return null;
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
                        owsReq.getExtensions().put(EXT_WS, true);

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
            
            try
            {
                sendResponse(request, capabilities);
                asyncCtx.complete();
            }
            catch (IOException e)
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
        new CapabilitiesUpdater(service, readDatabase).updateOfferings(capabilities);
        getLogger().debug("Updating capabilities");
        return capabilities;
    }   


    @Override
    protected void handleRequest(DescribeSensorRequest request) throws IOException, OWSException
    {
        String procUID = request.getProcedureID();

        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedure(procUID, report);
        checkQueryTime(request.getTime(), report);
        report.process();

        // security check
        securityHandler.checkPermission(procUID, securityHandler.sos_read_sensor);

        // create data provider
        var dataProvider = getDataProvider(procUID, request);

        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncProcedureSerializer serializer;
        if (format == null || SWESOfferingCapabilities.FORMAT_SML2.equals(format) || OWSUtils.XML_MIME_TYPE.equals(format))
            serializer = new ProcedureSerializerXml();
        else if (SWESOfferingCapabilities.FORMAT_SML2_JSON.equals(format) || OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new ProcedureSerializerJson();
        else
            throw new SOSException(SOSException.invalid_param_code, "procedureDescriptionFormat",
                format, "Unsupported procedure description format: " + format);

        // start async response
        AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        serializer.init(this, asyncCtx, request);
        dataProvider.getProcedureDescriptions(request, serializer);
    }


    @Override
    protected void handleRequest(GetObservationRequest request) throws IOException, OWSException
    {
        // set default format
        if (request.getFormat() == null)
            request.setFormat(GetObservationRequest.DEFAULT_FORMAT);
        
        // create data provider
        var dataProvider = getDataProvider(procUID);

        // build offering set (also from procedures ID)
        Set<String> selectedOfferings = new HashSet<>();
        for (String procID: request.getProcedures())
        {
            String offering = procedureToOfferingMap.get(procID);
            if (offering != null)
                selectedOfferings.add(offering);
        }
        if (selectedOfferings.isEmpty())
            selectedOfferings.addAll(request.getOfferings());
        else if (!request.getOfferings().isEmpty())
            selectedOfferings.retainAll(request.getOfferings());

        // if no offering or procedure specified scan all offerings
        if (selectedOfferings.isEmpty())
            selectedOfferings.addAll(offeringCaps.keySet());

        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryOfferings(request.getOfferings(), report);
        checkQueryObservables(request.getObservables(), report);
        checkQueryProcedures(request.getProcedures(), report);
        for (String offering: selectedOfferings)
            checkQueryFormat(offering, request.getFormat(), report);
        report.process();

        // security check
        for (String offeringID: selectedOfferings)
            securityHandler.checkPermission(offeringID, securityHandler.sos_read_obs);

        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncObsSerializer serializer;
        if (OWSUtils.XML_MIME_TYPE.equals(format) || format == null)
            serializer = new ObsSerializerXml();
        else if (OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new ObsSerializerJson();
        else
            throw new SOSException(SOSException.invalid_param_code, "procedureDescriptionFormat", format, "Procedure description format " + format + " is not supported");

        // create data providers for all selected offering
        // TODO sort by time by multiplexing obs from different offerings?
        LinkedList<ISOSAsyncDataProvider> dataProviders = new LinkedList<>();
        for (String offering: selectedOfferings)
            dataProviders.add(getDataProviderByOfferingID(offering));

        // start async response
        final AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        serializer.init(this, asyncCtx, request);

        // serialize data from each provider in sequence
        dataProviders.pop().getObservations(request, new DelegatingSubscriber<>(serializer) {
            boolean firstProvider = true;

            @Override
            public void onSubscribe(Subscription subscription)
            {
                if (firstProvider)
                {
                    firstProvider = false;
                    super.onSubscribe(subscription);
                }
            }

            @Override
            public void onComplete()
            {
                try
                {
                    ISOSAsyncDataProvider nextProvider = dataProviders.poll();
                    if (nextProvider != null)
                        nextProvider.getObservations(request, this);
                    else
                        super.onComplete();
                }
                catch (Exception e)
                {
                    onError(e);
                }
            }
        });
    }


    @Override
    protected void handleRequest(GetResultTemplateRequest request) throws IOException, OWSException
    {
        // security check
        securityHandler.checkPermission(request.getOffering(), securityHandler.sos_read_obs);

        // get data provider
        String procUID = getProcedureUID(request.getOffering());
        ISOSAsyncDataProvider dataProvider = getDataProvider(procUID, request);
        
        // start async response
        final AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        dataProvider.getResultTemplate(request).thenAccept(resultTemplate -> {
            try
            {
                // build filtered component tree, always keeping sampling time
                DataComponent filteredStruct = resultTemplate.getDataStructure().copy();
                request.getObservables().add(SWEConstants.DEF_SAMPLING_TIME);
                filteredStruct.accept(new DataStructFilter(request.getObservables()));

                // also add producer ID if needed
                if (dataProvider.hasMultipleProducers())
                    addProducerID(filteredStruct, "");

                // build and send response
                if (OWSUtils.JSON_MIME_TYPE.equals(request.getFormat()))
                {
                    OutputStream os = new BufferedOutputStream(request.getResponseStream());
                    SWEJsonStreamWriter writer = new SWEJsonStreamWriter(os, StandardCharsets.UTF_8.name());
                    request.getHttpResponse().setContentType(OWSUtils.JSON_MIME_TYPE);
                    SWEStaxBindings sweBindings = new SWEStaxBindings();
                    writer.writeStartDocument();
                    sweBindings.writeDataComponent(writer, filteredStruct, false);
                    writer.writeEndDocument();
                    writer.close();
                }
                else
                {
                    GetResultTemplateResponse resp = new GetResultTemplateResponse();
                    resp.setResultStructure(filteredStruct);
                    resp.setResultEncoding(resultTemplate.getDataEncoding());

                    // write response to response stream
                    OutputStream os = new BufferedOutputStream(request.getResponseStream());
                    owsUtils.writeXMLResponse(os, resp, request.getVersion(), request.getSoapVersion());
                    os.flush();
                }
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
        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedures(request.getProcedures(), report);
        checkQueryTime(request.getTime(), report);
        report.process();

        // security check
        securityHandler.checkPermission(request.getOffering(), securityHandler.sos_read_obs);
        boolean isWs = isWebSocketRequest(request);

        // create data provider
        String procUID = getProcedureUID(request.getOffering());
        ISOSAsyncDataProvider dataProvider = getDataProvider(procUID, request);
        
        // create GetResultTemplate request
        var grt = new GetResultTemplateRequest();
        grt.setOffering(request.getOffering());
        grt.setObservables(request.getObservables());
        
        // start async response
        final AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        dataProvider.getResultTemplate(grt).thenAccept(resultTemplate -> {
            try
            {
                // choose serializer according to output format
                ISOSAsyncResultSerializer serializer;
                if (OWSUtils.JSON_MIME_TYPE.equals(request.getFormat()))
                    serializer = new ResultSerializerJson();
                else if (OWSUtils.XML_MIME_TYPE.equals(request.getFormat()))
                    serializer = new ResultSerializerXml();
                else if (OWSUtils.BINARY_MIME_TYPE.equals(request.getFormat()))
                    serializer = new ResultSerializerBinary();
                else if (request.getFormat() != null)
                    serializer = getCustomFormatSerializer(request, resultTemplate);
                else
                    serializer = new ResultSerializerAuto();

                // subcribe and stream results asynchronously
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
        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedures(request.getProcedures(), report);
        report.process();

        // create data provider
        ISOSAsyncDataProvider dataProvider = getDataProvider(request);
        
        // choose serializer according to output format
        String format = request.getFormat();
        ISOSAsyncFeatureSerializer serializer;
        if (OWSUtils.XML_MIME_TYPE.equals(format) || format == null)
            serializer = new FeatureSerializerGml();
        else if (GeoJsonBindings.MIME_TYPE.equals(format) || OWSUtils.JSON_MIME_TYPE.equals(format))
            serializer = new FeatureSerializerGeoJson();
        else
            throw new SOSException(SOSException.invalid_param_code, "responseFormat",
                format, "Unsupported feature format: " + format);

        // start async response
        AsyncContext asyncCtx = request.getHttpRequest().startAsync();
        serializer.init(this, asyncCtx, request);
        dataProvider.getFeaturesOfInterest(request, serializer);
    }


    @Override
    protected void handleRequest(InsertSensorRequest request) throws IOException, OWSException
    {
        /*checkTransactionalSupport(request);

        // security check
        securityHandler.checkPermission(securityHandler.sos_insert_sensor);

        // check query parameters
        OWSExceptionReport report = new OWSExceptionReport();
        TransactionUtils.checkSensorML(request.getProcedureDescription(), report);
        report.process();

        // get sensor UID
        String sensorUID = request.getProcedureDescription().getUniqueIdentifier();
        log.info("Registering new sensor {}", sensorUID);

        // offering name is derived from sensor UID
        String offeringID = sensorUID + "-sos";

        ///////////////////////////////////////////////////////////////////////////////////////
        // we configure things step by step so we can fix config if it was partially altered //
        ///////////////////////////////////////////////////////////////////////////////////////
        HashSet<ModuleConfig> configSaveList = new HashSet<>();
        IProcedureRegistry procReg = service.getParentHub().getProcedureRegistry();

        // create new virtual sensor module if needed
        IProcedureWithState proc = procReg.get(sensorUID);
        if (proc == null)
        {
            proc = new ProcedureProxyImpl(request.getProcedureDescription());//, groupUID);
            procReg.register(proc);
        }
        // else simply update description
        else
            ((ProcedureProxyImpl)proc).updateDescription(request.getProcedureDescription());

        // add new provider if needed
        ISOSDataProviderFactory provider = dataProviders.get(offeringID);
        if (provider == null)
        {
            // generate new provider config
            SensorDataProviderConfig providerConfig = new SensorDataProviderConfig();
            providerConfig.enabled = true;
            providerConfig.sensorID = sensorUID;
            providerConfig.storageID = (storageModule != null) ? storageModule.getLocalID() : null;
            providerConfig.offeringID = offeringID;
            providerConfig.liveDataTimeout = config.defaultLiveTimeout;
            OfferingUtils.replaceOrAddOfferingConfig(config.dataProviders, providerConfig);

            // instantiate and register provider
            try
            {
                provider = providerConfig.getFactory(this);
                dataProviders.put(offeringID, provider);
            }
            catch (SensorHubException e)
            {
                throw new IOException("Cannot create new provider", e);
            }

            // add new permissions for this offering
            securityHandler.addOfferingPermissions(offeringID);

            configSaveList.add(config);
        }

        // update capabilities
        showProviderCaps(provider);

        // build and send response
        InsertSensorResponse resp = new InsertSensorResponse();
        resp.setAssignedOffering(offeringID);
        resp.setAssignedProcedureId(sensorUID);
        sendResponse(request, resp);*/
    }


    @Override
    protected void handleRequest(DeleteSensorRequest request) throws IOException, OWSException
    {
        /*checkTransactionalSupport(request);

        // check query parameters
        String sensorUID = request.getProcedureId();
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedure(sensorUID, report);
        report.process();

        // security check
        securityHandler.checkPermission(sensorUID, securityHandler.sos_delete_sensor);

        // destroy associated virtual sensor
        try
        {
            // unregister procedure
            ProcedureProxyImpl proxy = getProcedureProxyByUID(sensorUID);
            service.getParentHub().getProcedureRegistry().unregister(proxy);

            // delete all data from databaseID

        }
        catch (SensorHubException e)
        {
            throw new IOException("Cannot delete virtual sensor " + sensorUID, e);
        }

        // TODO also destroy storage if requested in config

        // build and send response
        DeleteSensorResponse resp = new DeleteSensorResponse(SOSUtils.SOS);
        resp.setDeletedProcedure(sensorUID);
        sendResponse(request, resp);*/
    }


    @Override
    protected void handleRequest(UpdateSensorRequest request) throws IOException, OWSException
    {
        /*checkTransactionalSupport(request);

        // check query parameters
        String procUID = request.getProcedureId();
        OWSExceptionReport report = new OWSExceptionReport();
        checkQueryProcedure(procUID, report);
        report.process();

        // security check
        securityHandler.checkPermission(procUID, securityHandler.sos_update_sensor);

        // check that format is supported
        checkQueryProcedureFormat(procUID, request.getProcedureDescriptionFormat(), report);

        // check that SensorML contains correct unique ID
        TransactionUtils.checkSensorML(request.getProcedureDescription(), report);
        report.process();

        // get consumer and update
        ProcedureProxyImpl proc = getProcedureProxyByUID(procUID);
        proc.updateDescription(request.getProcedureDescription());

        // build and send response
        UpdateSensorResponse resp = new UpdateSensorResponse(SOSUtils.SOS);
        resp.setUpdatedProcedure(procUID);
        sendResponse(request, resp);*/
    }


    @Override
    protected void handleRequest(InsertObservationRequest request) throws IOException, OWSException
    {
        /*checkTransactionalSupport(request);

        // retrieve proxy for selected offering
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
        /*checkTransactionalSupport(request);

        // retrieve proxy for selected offering
        ProcedureProxyImpl proxy = getProcedureProxyByOfferingID(request.getOffering());
        SensorDataConsumer consumer = new SensorDataConsumer(proxy);

        // security check
        securityHandler.checkPermission(proxy.getUniqueIdentifier(), securityHandler.sos_insert_obs);

        // get template ID
        // the same template ID is always returned for a given observable
        String templateID = consumer.newResultTemplate(request.getResultStructure(),
                                                       request.getResultEncoding(),
                                                       request.getObservationTemplate());

        // update caps only if template was not already registered
        if (!templateToProcedureMap.containsKey(templateID))
        {
            templateToProcedureMap.put(templateID, proxy.getUniqueIdentifier());

            // update offering capabilities
            showProviderCaps(proxy);
        }

        // build and send response
        InsertResultTemplateResponse resp = new InsertResultTemplateResponse();
        resp.setAcceptedTemplateId(templateID);
        sendResponse(request, resp);*/
    }


    @Override
    protected void handleRequest(InsertResultRequest request) throws IOException, OWSException
    {
        /*DataStreamParser parser = null;

        checkTransactionalSupport(request);

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


    protected void checkQueryProcedures(Set<String> procedures, OWSExceptionReport report) throws SOSException
    {
        for (String procUID: procedures)
            checkQueryProcedure(procUID, report);
    }


    protected void checkQueryProcedure(String procUID, OWSExceptionReport report) throws SOSException
    {
        if (procUID == null || !readDatabase.getProcedureStore().contains(procUID))
            report.add(new SOSException(SOSException.invalid_param_code, "procedure", procUID, "Unknown procedure: " + procUID));
    }


    protected void checkQueryFormat(String format, OWSExceptionReport report) throws SOSException
    {
        if (!getFirstOffering().getResponseFormats().contains(format))
            report.add(new SOSException(SOSException.invalid_param_code, "format", format, "Unsupported format: " + format));
    }
    
    
    protected void checkQueryTime(TimeExtent requestTime, OWSExceptionReport report) throws SOSException
    {
        // reject null time period
        if (requestTime == null)
            return;
        
        // reject if startTime > stopTime
        if (requestTime.end().isBefore(requestTime.begin()))
            report.add(new SOSException("The requested period must begin before it ends"));            
    }
    
    
    protected SOSOfferingCapabilities getFirstOffering() throws SOSException
    {
        var offering = capabilities.getLayers().get(0);
        if (offering == null)
            throw new SOSException("No data available on this server");
        return offering;
    }


    protected ISOSAsyncDataProvider getDataProvider(String procUID, OWSRequest request) throws IOException, OWSException
    {
        try
        {
            SOSProviderConfig config = providerConfigs.get(procUID);

            // if no custom config was provided use default
            if (config == null)
                config = new ProcedureDataProviderConfig();

            return config.createProvider(service, request);
        }
        catch (SensorHubException e)
        {
            throw new IOException("Cannot get provider for procedure " + procUID, e);
        }
    }
    
    
    protected ISOSAsyncDataProvider getDataProvider(OWSRequest request) throws IOException, OWSException
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
        if (!config.enableTransactional)
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

        if (userAgent.contains("Firefox"))
            return true;
        if (userAgent.contains("Chrome"))
            return true;

        return false;
    }


    /*
     * Check if request is through websocket protocol
     */
    protected boolean isWebSocketRequest(OWSRequest request)
    {
        return request.getExtensions().containsKey(EXT_WS);
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
        }

        // try to find a matching implementation for selected format
        ISOSAsyncResultSerializer serializer;
        if (format != null && (serializer = customFormats.get(format)) != null)
            return serializer;

        throw new SOSException(SOSException.invalid_param_code, "format", format, "Unsupported format " + format);
    }


    public String getProcedureUID(String offeringID)
    {
        // for now, assume offerings have same URI as procedures
        return offeringID;
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
