/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.felix.bundlerepository.RepositoryAdmin;
import org.apache.felix.bundlerepository.Resource;
import org.apache.felix.bundlerepository.impl.RequirementImpl;
import org.osgi.framework.BundleContext;
import org.osgi.service.repository.Repository;
import org.vast.xml.DOMHelper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.vaadin.v7.data.Item;
import com.vaadin.server.ExternalResource;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Link;
import com.vaadin.ui.ProgressBar;
import com.vaadin.v7.ui.Table;
import ch.qos.logback.core.recovery.ResilientSyslogOutputStream;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;


/**
 * <p>
 * Popup window showing a list of installable OSH bundles
 * </p>
 *
 * @author Alex Robin
 * @since April 10, 2021
 */
@SuppressWarnings("serial")
public class DownloadOsgiBundlesPopup extends Window
{
    private static final String LOADING_MSG = "Loading...";
    
    Table table;
    transient ExecutorService exec = Executors.newFixedThreadPool(2);
    
    
    public DownloadOsgiBundlesPopup(BundleContext osgiCtx)
    {
        super("Download Add-on Modules");
        setModal(true);
                
        final VerticalLayout layout = new VerticalLayout();
        layout.setMargin(true);
        layout.setSpacing(true);
                
        final HorizontalLayout loading = new HorizontalLayout();
        loading.setSpacing(true);
        ProgressBar pb = new ProgressBar();
        pb.setIndeterminate(true);
        loading.addComponent(pb);
        loading.addComponent(new Label("Loading Package Information..."));
        layout.addComponent(loading);
        
        setContent(layout);
        center();
        
        // load info in separate thread
        exec.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    /*var ref = osgiCtx.getServiceReference(Repository.class);
                    var repo = osgiCtx.getService(ref);
                    //Requirement req = new RequirementImpl();
                    var req = repo..newRequirementBuilder("org.sensorhub")
                        .addDirective("osgi.identity", "(osgi.identity=org.sensorhub.*)")
                        .build();
                    var res = repo.findProviders(Arrays.asList(req));
                    System.out.println(res);*/
                    
                    var ref = osgiCtx.getServiceReference(RepositoryAdmin.class);
                    var repo = osgiCtx.getService(ref);
                    
                    repo.addRepository("file:///home/alex/Projects/Workspace_OSH_V2/osh-core/build/osgi/index.xml");
                    
                    var resolver = repo.resolver();
                    var resources = repo.discoverResources("(symbolicname=*)");
                    for (var res: resources)
                    {
                        System.out.println(res);
                        updateTable(res, layout);
                    }
                    
                    /*resolver.add(resource);
                    if (resolver.resolve())
                    {
                        resolver.deploy(true);
                    }
                    else
                    {
                        Requirement[] reqs = resolver.getUnsatisfiedRequirements();
                        for (int i = 0; i < reqs.length; i++)
                        {
                            System.out.println("Unable to resolve: " + reqs[i]);
                        }
                    }*/
                    
                    
                    // remove loading indicator when done
                    getUI().access(new Runnable() {
                        @Override
                        public void run()
                        {
                            layout.removeComponent(loading);
                            getUI().push();
                        }
                    });
                }
                catch (final Exception e)
                {
                    final UI ui = getUI();
                    ui.access(new Runnable() {
                        @Override
                        public void run()
                        {
                            DisplayUtils.showErrorPopup("Cannot fetch OSH package list", e);
                            DownloadOsgiBundlesPopup.this.close();
                            ui.push();
                        }
                    });
                }
            }
        });
    }
    
    
    protected void updateTable(final Resource pkg, final VerticalLayout layout)
    {
        // update table in UI thread
        getUI().access(new Runnable() {
            @Override
            public void run()
            {
                // create table if not there yet
                if (table == null)
                {
                    setWidth(70, Unit.PERCENTAGE);
                    table = new Table();
                    table.setSizeFull();      
                    table.setSelectable(false);
                    table.addContainerProperty(ModuleTypeSelectionPopup.PROP_NAME, String.class, null);
                    table.addContainerProperty(ModuleTypeSelectionPopup.PROP_VERSION, String.class, null);
                    table.addContainerProperty(ModuleTypeSelectionPopup.PROP_DESC, String.class, null);
                    table.addContainerProperty(ModuleTypeSelectionPopup.PROP_AUTHOR, String.class, null);
                    table.setColumnHeaders(new String[] {"Bundle", "Version", "Description", "Author"});
                    table.setColumnWidth(ModuleTypeSelectionPopup.PROP_NAME, 300);
                    table.setColumnExpandRatio(ModuleTypeSelectionPopup.PROP_DESC, 10);
                    layout.addComponent(table, 0);
                }
                
                // add package info
                String href = pkg.getURI();
                Link link = new Link(pkg.getPresentationName(), new ExternalResource(href), null, 0, 0, null);
                
                var description = pkg.getProperties().get(Resource.DESCRIPTION);
                var copyright = pkg.getProperties().get(Resource.COPYRIGHT);
                var license = pkg.getProperties().get(Resource.LICENSE_URI);
                var id = table.addItem(new Object[] {pkg.getPresentationName(), pkg.getVersion().toString(), description, copyright}, href);
                                
                getUI().push();
            }
        });
    }
    
}
