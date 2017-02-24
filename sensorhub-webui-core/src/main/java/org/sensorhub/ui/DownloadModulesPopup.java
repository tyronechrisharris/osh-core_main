/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.ui;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import org.vast.xml.DOMHelper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.vaadin.server.ExternalResource;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Link;
import com.vaadin.ui.Notification;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.TreeTable;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;


/**
 * <p>
 * Popup window showing a list of downloadable OSH modules
 * </p>
 *
 * @author Alex Robin <alex.robin@sensiasoftware.com>
 * @since Feb 24, 2017
 */
public class DownloadModulesPopup extends Window
{
    private static final long serialVersionUID = 2104928069934026519L;
    private static final String BINTRAY_API_ROOT = "https://api.bintray.com/";
    private static final String BINTRAY_SUBJECT = "sensiasoft";
    private static final String BINTRAY_REPO = "osh";
    private static final String BINTRAY_PKG_LIST = "repos/" + BINTRAY_SUBJECT + "/" + BINTRAY_REPO + "/packages";
    private static final String BINTRAY_PKG = "packages/" + BINTRAY_SUBJECT + "/" + BINTRAY_REPO + "/";
    private static final String BINTRAY_WEB_ROOT = "https://bintray.com/" + BINTRAY_SUBJECT + "/" + BINTRAY_REPO + "/";
    private static final String BINTRAY_CONTENT_ROOT = "https://dl.bintray.com/" + BINTRAY_SUBJECT + "/" + BINTRAY_REPO + "/";
    private static final String LINK_TARGET = "osh-bintray";
    
    TreeTable table;
    
    
    static class BintrayArtifact
    {
        public String name;
        public String path;
        public String label;
        public String desc;
        public String version;
        public String author;
    }
    
    
    static class BintrayPackage
    {
        public String name;
        public String desc;
        public String website_url;
        public String updated;
        public Collection<BintrayArtifact> files;
    }
    
    
    public DownloadModulesPopup()
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
        Thread t = new Thread()
        {
            public void run()
            {
                try
                {
                    for (final String name: getBintrayPackageNames())
                    {
                        try
                        {
                            final BintrayPackage pkg = getBintrayPackageInfo(name);
                            pkg.files =  getBintrayPackageFiles(name);
                            
                            getUI().access(new Runnable() {
                                public void run()
                                {
                                    // create table if not there yet
                                    if (table == null)
                                    {
                                        setWidth(70, Unit.PERCENTAGE);
                                        table = new TreeTable();
                                        table.setSizeFull();      
                                        table.setSelectable(false);
                                        table.addContainerProperty("name", Component.class, null);
                                        table.addContainerProperty("desc", String.class, null);
                                        table.addContainerProperty("version", String.class, null);
                                        table.addContainerProperty("author", String.class, null);
                                        table.setColumnHeaders(new String[] {"Add-On Name", "Description", "Version", "Author"});
                                        table.setColumnWidth("name", 300);
                                        table.setColumnExpandRatio("desc", 10);
                                        layout.addComponent(table, 0);
                                    }
                                    
                                    // add package info
                                    String href = BINTRAY_WEB_ROOT + pkg.name; 
                                    Link link = new Link(pkg.name, new ExternalResource(href), LINK_TARGET, 0, 0, null);
                                    Object parentId = table.addItem(new Object[] {link, pkg.desc, null, null}, null);    
                                                                    
                                    // add all jar files
                                    for (BintrayArtifact f: pkg.files)
                                    {
                                        if (f.name.endsWith("jar") && !f.name.endsWith("-sources.jar") && !f.name.endsWith("-javadoc.jar"))
                                        {
                                            href = BINTRAY_CONTENT_ROOT + f.path; 
                                            link = new Link(f.label, new ExternalResource(href), "_self", 0, 0, null);
                                            Object id = table.addItem(new Object[] {link, f.desc, f.version, f.author}, null);
                                            table.setParent(id, parentId);
                                            table.setChildrenAllowed(id, false);
                                        }
                                    }
                                    
                                    getUI().push();
                                }
                            });
                        }
                        catch (Exception e)
                        {
                            getUI().access(new Runnable() {
                                public void run()
                                {
                                    table.addItem(new Object[] {null, "Error loading package " + name, null, null}, null); 
                                    getUI().push();
                                }
                            });
                        }
                    }
                    
                    // remove loading indicator when done
                    getUI().access(new Runnable() {
                        public void run()
                        {
                            layout.removeComponent(loading);
                            getUI().push();
                        }
                    });
                }
                catch (Exception e)
                {
                    getUI().access(new Runnable() {
                        public void run()
                        {
                            Notification.show("Error", "Cannot fetch OSH package list", Notification.Type.ERROR_MESSAGE);
                            getUI().push();
                        }
                    });
                }
            }
        };
        t.start();
    }
    
    
    protected Collection<String> getBintrayPackageNames() throws Exception
    {
        // get OSH package names
        ArrayList<String> pkgNames = new ArrayList<String>();
        URL url = new URL(BINTRAY_API_ROOT + BINTRAY_PKG_LIST);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream())))
        {
            JsonParser parser = new JsonParser();
            JsonArray root = (JsonArray)parser.parse(reader);
            for (JsonElement elt: root)
            {
                if (elt.isJsonObject())
                {
                    JsonElement name = ((JsonObject)elt).get("name");
                    if (name != null)
                        pkgNames.add(name.getAsString());
                }
            }
        }
        
        return pkgNames;
    }
    
    
    protected BintrayPackage getBintrayPackageInfo(String pkgName) throws Exception
    {
        URL url = new URL(BINTRAY_API_ROOT + BINTRAY_PKG + pkgName);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream())))
        {
            Gson gson = new Gson();
            return gson.fromJson(reader, BintrayPackage.class);
        }
    }
    
    
    protected Collection<BintrayArtifact> getBintrayPackageFiles(String pkgName) throws Exception
    {
        Collection<BintrayArtifact> files;
        
        URL url = new URL(BINTRAY_API_ROOT + BINTRAY_PKG + pkgName + "/files");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream())))
        {
            Gson gson = new Gson();
            Type collectionType = new TypeToken<Collection<BintrayArtifact>>(){}.getType();
            files = gson.fromJson(reader, collectionType);
        }
        
        // also load info from POM
        for (BintrayArtifact file: files)
        {
            String pomUrl = BINTRAY_CONTENT_ROOT + file.path.replaceAll(".jar", ".pom");
            readInfoFromPom(pomUrl, file);
        }
        
        return files;
    }
    
    
    protected void readInfoFromPom(String url, BintrayArtifact item) throws Exception
    {
        try
        {
            DOMHelper dom = new DOMHelper(url, false);
            
            String pomName = dom.getElementValue("name");
            if (pomName != null && !pomName.isEmpty())
                item.label = pomName;
            else
                item.label = item.name;
            item.desc = dom.getElementValue("description");
            item.author = dom.getElementValue("developers/developer/organization");
        }
        catch (Exception e)
        {
            System.err.println("Error while reading POM at " + url);
        }
    }
    
}
