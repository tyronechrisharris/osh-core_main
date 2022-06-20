/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.home;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.sensorhub.impl.service.sweapi.BaseHandler;
import org.sensorhub.impl.service.sweapi.InvalidRequestException;
import org.sensorhub.impl.service.sweapi.ServiceErrors;
import org.sensorhub.impl.service.sweapi.feature.FoiHandler;
import org.sensorhub.impl.service.sweapi.procedure.ProcedureHandler;
import org.sensorhub.impl.service.sweapi.resource.IResourceHandler;
import org.sensorhub.impl.service.sweapi.resource.RequestContext;
import org.sensorhub.impl.service.sweapi.resource.ResourceBinding;
import org.sensorhub.impl.service.sweapi.resource.ResourceFormat;
import org.sensorhub.impl.service.sweapi.resource.ResourceLink;
import org.sensorhub.impl.service.sweapi.system.SystemHandler;
import org.vast.swe.SWEConstants;


public class CollectionHandler extends BaseHandler
{
    public static final String[] NAMES = { "collections" };
    
    Map<String, CollectionInfo> allCollections = new LinkedHashMap<>();
    
    
    static class CollectionInfo
    {
        String id;
        String title;
        String description;
        String attribution;
        String itemType = "feature";
        String featureType;
        Set<String> keywords = new LinkedHashSet<>();
        Set<String> crs = new LinkedHashSet<>();
        Set<ResourceLink> links = new LinkedHashSet<>();
    }
    
    
    public CollectionHandler()
    {
    }
    
    
    @Override
    public String[] getNames()
    {
        return NAMES;
    }
    
    
    @Override
    public void doGet(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        // check permissions
        //ctx.getSecurityHandler().checkPermission(permissions.read);
        
        // parse format
        var format = parseFormat(ctx.getParameterMap());
        ctx.setFormatOptions(format, null);
        
        // if requesting from this resource collection
        if (ctx.isEndOfPath())
        {
            list(ctx);
            return;
        }
        
        // otherwise there should be a specific collection ID
        String id = ctx.popNextPathElt();
        if (ctx.isEndOfPath())
        {
            getById(ctx, id);
            return;
        }
        
        // next should be nested resource
        IResourceHandler resource = getSubResource(ctx);
        if (resource != null)
            resource.doGet(ctx);
        else
            throw ServiceErrors.badRequest(INVALID_URI_ERROR_MSG);
    }
    
    
    protected ResourceBinding<String, CollectionInfo> getBinding(RequestContext ctx, boolean forReading) throws IOException
    {
        var format = ctx.getFormat();
        if (format.equals(ResourceFormat.HTML) ||
           (format.equals(ResourceFormat.AUTO) && ctx.isBrowserHtmlRequest()))
        {
            ctx.setResponseFormat(ResourceFormat.HTML);
            return new CollectionHtml(ctx);
        }
        else if (format.isOneOf(ResourceFormat.AUTO, ResourceFormat.JSON))
        {
            ctx.setResponseFormat(ResourceFormat.JSON);
            return new CollectionJson(ctx);
        }
        else
            throw ServiceErrors.unsupportedFormat(format);
    }
    
    
    protected Map<String, CollectionInfo> getCollections(RequestContext ctx)
    {
        if (allCollections.isEmpty())
        {
            // systems collection
            var systemCol = new CollectionInfo();
            systemCol.id = "all_systems";
            systemCol.title = "All Connected Systems";
            systemCol.description = "All systems registered on this server (e.g. platforms, sensors, actuators, processes)";
            systemCol.featureType = "system";
            systemCol.links.add(ResourceLink.self(
                ctx.getApiRootURL() + "/" + NAMES[0],
                ResourceFormat.JSON.getMimeType()));
            addItemsLink("Access the system instances", SystemHandler.NAMES[0], ctx, systemCol.links);
            allCollections.put(systemCol.id, systemCol);
            
            // datastreams collection
            var dsCol = new CollectionInfo();
            dsCol.id = "all_datastreams";
            dsCol.title = "All Systems Datastreams";
            dsCol.description = "All datastreams produced by systems registered on this server";
            dsCol.featureType = "datastreams";
            dsCol.crs.add(SWEConstants.REF_FRAME_CRS84h);
            dsCol.links.add(ResourceLink.self(
                ctx.getApiRootURL() + "/" + NAMES[0],
                ResourceFormat.JSON.getMimeType()));
            addItemsLink("Access the datastreams", FoiHandler.NAMES[0], ctx, dsCol.links);
            allCollections.put(dsCol.id, dsCol);
            
            // features of interest collection
            var foiCol = new CollectionInfo();
            foiCol.id = "all_fois";
            foiCol.title = "All Features of Interest";
            foiCol.description = "All features of interest observed or affected by systems registered on this server";
            foiCol.featureType = "featureOfInterest";
            foiCol.crs.add(SWEConstants.REF_FRAME_CRS84h);
            foiCol.links.add(ResourceLink.self(
                ctx.getApiRootURL() + "/" + NAMES[0],
                ResourceFormat.JSON.getMimeType()));
            addItemsLink("Access the features of interests", FoiHandler.NAMES[0], ctx, foiCol.links);
            allCollections.put(foiCol.id, foiCol);
            
            /*// system types collection
            var systemTypeCol = new CollectionInfo();
            systemTypeCol.id = "all_system_types";
            systemTypeCol.title = "All System Types";
            systemTypeCol.description = "All types/models of systems registered on this server";
            systemTypeCol.featureType = "systemType";
            systemTypeCol.crs.add(SWEConstants.REF_FRAME_CRS84h);
            systemTypeCol.links.add(ResourceLink.self(
                ctx.getApiRootURL() + "/" + NAMES[0],
                ResourceFormat.JSON.getMimeType()));
            addItemsLink("Access the system types", ProcedureHandler.NAMES[0], ctx, systemTypeCol.links);
            allCollections.put(systemTypeCol.id, systemTypeCol);*/
        }
        
        return allCollections;
    }
    
    
    protected void addItemsLink(String title, String path, RequestContext ctx, Set<ResourceLink> links)
    {
        links.add(ResourceLink.builder()
            .rel(ResourceLink.REL_ITEMS)
            .title(title + " in this collection")
            .type(ResourceFormat.JSON.getMimeType())
            .href(ctx.getApiRootURL() + "/" + path)
            .build());
    }
    
    
    void list(RequestContext ctx) throws IOException
    {
        var binding = getBinding(ctx, false);
        binding.startCollection();
        
        // fetch collections info
        for (var col: getCollections(ctx).values())
            binding.serialize(col.id, col, false);
        
        // add default links
        var links = new ArrayList<ResourceLink>();
        links.add(ResourceLink.self(
            ctx.getApiRootURL() + "/collections?f=" + ctx.getFormat().getMimeType(),
            ctx.getFormat().getMimeType()
        ));
        
        if (ctx.getFormat().equals(ResourceFormat.JSON))
        {
            links.add(ResourceLink.builder()
                .rel("alternate")
                .title("This document as HTML")
                .type(ResourceFormat.HTML.getMimeType())
                .href(ctx.getApiRootURL() + "/collections?f=" + ResourceFormat.HTML.getMimeType())
                .build());
        }
        else
        {
            links.add(ResourceLink.builder()
                .rel("alternate")
                .title("This document as JSON")
                .type(ResourceFormat.HTML.getMimeType())
                .href(ctx.getApiRootURL() + "/collections?f=" + ResourceFormat.JSON.getMimeType())
                .build());
        }
        
        binding.endCollection(links);
    }
    
    
    void getById(RequestContext ctx, String id) throws IOException
    {
        var binding = getBinding(ctx, false);
        
        var col = getCollections(ctx).get(id);
        if (col == null)
            throw ServiceErrors.notFound(id);
        
        binding.serialize(id, col, true);
    }
    
    
    @Override
    public void doPost(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        ServiceErrors.unsupportedOperation("");
    }
    

    @Override
    public void doPut(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        ServiceErrors.unsupportedOperation("");
    }
    

    @Override
    public void doDelete(RequestContext ctx) throws InvalidRequestException, IOException, SecurityException
    {
        ServiceErrors.unsupportedOperation("");
    }

}
