/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2017 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.utils;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;


public class Async
{
    
    public static void waitForCondition(Supplier<Boolean> condition, long timeout) throws TimeoutException
    {
        waitForCondition(condition, 100L, timeout);
    }
    
    
    public static void waitForCondition(Supplier<Boolean> condition, long retryInterval, long timeout) throws TimeoutException
    {
        try
        {
            long t0 = System.currentTimeMillis();
            
            synchronized(condition)
            {
                while (!condition.get())
                {
                    if (System.currentTimeMillis() > t0+timeout)
                        throw new TimeoutException();
                    condition.wait(retryInterval);
                }
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
    
}
