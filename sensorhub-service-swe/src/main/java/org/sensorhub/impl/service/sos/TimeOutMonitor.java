/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sos;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * <p>
 * TODO TimeOutMonitor type description
 * </p>
 *
 * @author Alex Robin
 * @date Apr 15, 2020
 */
public class TimeOutMonitor
{
    ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    Set<Runnable> triggers = Collections.synchronizedSet(
        Collections.newSetFromMap(new WeakHashMap<>()));
    
    
    public TimeOutMonitor()
    {
        // schedule to run all triggers every 10 seconds by default
        this(10000);
    }
    
    
    /**
     * @param resolution resolution of timeout detection in millis
     */
    public TimeOutMonitor(int resolution)
    {
        timer.scheduleWithFixedDelay(this::triggerAll, 0, resolution, TimeUnit.MILLISECONDS);
    }
    
    
    protected void triggerAll()
    {
        for (Runnable trigger: triggers)
            trigger.run();
    }
    
    
    public void register(Runnable trigger)
    {
        triggers.add(trigger);
    }
    
    
    public void unregister(Runnable trigger)
    {
        triggers.remove(trigger);
    }
}
