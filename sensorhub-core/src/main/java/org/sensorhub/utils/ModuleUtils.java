/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2016 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.utils;

import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.sensorhub.api.module.IModule;
import org.sensorhub.api.module.IModuleProvider;
import org.sensorhub.api.module.ModuleConfig;


public class ModuleUtils
{
    public final static String MODULE_NAME = "Bundle-Name";
    public final static String MODULE_DESC = "Bundle-Desc";
    public final static String MODULE_VERSION = "Bundle-Version";
    public final static String MODULE_VENDOR = "Bundle-Vendor";
    
    
    private static Manifest getManifest(Class<?> clazz)
    {
        try
        {
            String jarUrl = clazz.getProtectionDomain().getCodeSource().getLocation().getFile();
            try ( JarFile jar = new JarFile(new File(jarUrl)) ) {
                return jar.getManifest();
            }
        }
        catch (IOException e)
        {
            return null;
        }
    }
    
    
    public static IModuleProvider getModuleInfo(Class<?> clazz)
    {
        Manifest manifest = getManifest(clazz);
        final String name, desc, version, vendor;
        
        if (manifest == null)
        {
            name = MODULE_NAME;
            desc = MODULE_DESC;
            version = MODULE_VERSION;
            vendor = MODULE_VENDOR;
        }
        else
        {
            Attributes attributes = manifest.getMainAttributes();
            name = attributes.getValue(MODULE_NAME);
            desc = attributes.getValue(MODULE_DESC);
            version = attributes.getValue(MODULE_VERSION);
            vendor = attributes.getValue(MODULE_VENDOR);
        }
        
        return new IModuleProvider()
        {
            @Override
            public String getModuleName()
            {
                return name;
            }

            @Override
            public String getModuleDescription()
            {
                return desc;
            }

            @Override
            public String getModuleVersion()
            {
                return version;
            }

            @Override
            public String getProviderName()
            {
                return vendor;
            }

            @Override
            public Class<? extends IModule<?>> getModuleClass()
            {
                return null;
            }

            @Override
            public Class<? extends ModuleConfig> getModuleConfigClass()
            {
                return null;
            }    
        };
    }
}
