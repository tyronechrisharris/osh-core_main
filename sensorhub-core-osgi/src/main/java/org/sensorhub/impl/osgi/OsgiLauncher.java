/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.osgi;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.util.HashMap;
import java.util.ServiceLoader;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.FrameworkFactory;


public class OsgiLauncher
{
    static final String BUNDLE_FOLDER = "bundles";
    BundleContext systemCtx;
    
    
    public OsgiLauncher() throws Exception
    {
        var frameworkFactory = ServiceLoader.load(FrameworkFactory.class).iterator().next();
        var config = new HashMap<String,String>();
        
        config.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        config.put(Constants.FRAMEWORK_SYSTEMPACKAGES,
            "org.osgi.framework; version=1.10.0," +
            //"org.osgi.framework.dto; version=1.8.0," +
            "org.osgi.framework.wiring; version=1.2.0," +
            //"org.osgi.dto; version=1.1.0," +
            "org.osgi.resource; version=1.0.0," +
            "org.osgi.util.tracker; version=1.5.2," +
            "org.osgi.service.packageadmin; version=1.2.0," +
            "org.osgi.service.startlevel; version=1.1.0," +
            "org.osgi.service.url; version=1.0.0");
        
        config.put(Constants.FRAMEWORK_BOOTDELEGATION,
            "javax.*,sun.*,com.sun.*," +
            "org.xml.*,org.w3c.*");
        
        var framework = frameworkFactory.newFramework(config);
        
        // register shutdown hook for a clean stop 
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try
            {
                framework.stop();
            }
            catch (BundleException e)
            {
                e.printStackTrace();
            }
        }));        
        
        framework.start();
        systemCtx = framework.getBundleContext();
        Bundle bundle;
        
        // autostart everything in bundles folder
        File dir = new File(BUNDLE_FOLDER);
        File[] bundleJarFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });
        for (var f: bundleJarFiles)
            newBundle(f.toPath());
        
        // install all bundles
        var coreBundle = bundle = systemCtx.installBundle("reference:file:../sensorhub-core/build/libs/sensorhub-core-2.0.0-bundle.jar");
        bundle.start();
        
        bundle = systemCtx.installBundle("reference:file:../sensorhub-service-swe/build/libs/sensorhub-service-swe-2.0.0-bundle.jar");
        bundle.start();
        
        bundle = systemCtx.installBundle("reference:file:../sensorhub-webui-core/build/libs/sensorhub-webui-core-2.0.0-bundle.jar");
        bundle.start();
        
        //bundle = systemCtx.installBundle("reference:file:../sensorhub-datastore-h2/build/libs/sensorhub-datastore-h2-2.0.0-bundle.jar");
        //bundle.start();
        
        // watch bundle folder
        try
        {
            var watcher = new DirectoryWatcher(Paths.get(BUNDLE_FOLDER), StandardWatchEventKinds.ENTRY_CREATE);
            var watcherThread = new Thread(watcher);
            watcher.addListener(this::newBundle);
            watcherThread.start();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
        // start sensor hub
        var ref = coreBundle.getBundleContext().getServiceReferences(Runnable.class, "(type=ISensorHub)").stream().findFirst().get();
        //var ref = coreBundle.getBundleContext().getServiceReference(Runnable.class);
        var hub = systemCtx.getService(ref);
        hub.run();
        
    }
    
    
    public void newBundle(Path path)
    {
        try
        {
            var bundle = systemCtx.installBundle("reference:file:" + path.toString());
            bundle.start();
        }
        catch (BundleException e)
        {
            e.printStackTrace();
        }
    }
    
    
    public static void main(String[] args) throws Exception
    {
        // create bundles directory
        new File(BUNDLE_FOLDER).mkdir();
        
        System.getProperties().setProperty("osh.config", "/home/alex/Projects/Workspace_OSH_V2/osh-core/sensorhub-core-test/src/main/resources/config_empty_sost.json");
        new OsgiLauncher();
    }
}
