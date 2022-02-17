/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2015 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl;

import java.util.Hashtable;
import java.util.Map;
import java.util.function.Consumer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.sensorhub.api.ISensorHub;
import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.utils.OshBundleActivator;


public class Activator extends OshBundleActivator implements BundleActivator
{
    ISensorHub hub;
    
    
    @Override
    public void start(BundleContext context) throws Exception
    {
        super.start(context);
        
        // register SensorHub instance as a generic service
        // we use a Consumer interface since the ISensorHub interface is not available to the OSGi framework
        var props = new Hashtable<String, String>();
        props.put("type", "ISensorHub");
        context.registerService(Consumer.class, new Consumer<Map<String,String>>() {
            @Override
            public void accept(Map<String,String> args)
            {
                try
                {
                    SensorHubConfig config = new SensorHubConfig(args.get("config"), null);
                    hub = new SensorHub(config, context);
                    hub.start();
                }
                catch (SensorHubException e)
                {
                    e.printStackTrace();
                }
            }
        }, props);
    }


    @Override
    public void stop(BundleContext context) throws Exception
    {
        super.stop(context);
        
        if (hub != null)
            hub.stop();
    }

}
