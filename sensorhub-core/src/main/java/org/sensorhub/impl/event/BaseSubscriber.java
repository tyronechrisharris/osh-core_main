/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2019, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.event;

import java.util.concurrent.Flow.Subscriber;
import org.slf4j.Logger;


/**
 * <p>
 * Base class to simplify implementations of subscribers
 * </p>
 *
 * @author Alex Robin
 * @param <T> 
 * @date Feb 24, 2019
 */
public abstract class BaseSubscriber<T> implements Subscriber<T>
{
    String name;
    Logger log;
    
    
    public BaseSubscriber(String name, Logger log)
    {
        this.name = name;
        this.log = log;
    }
    
    
    @Override
    public void onComplete()
    {
        log.info("Subscriber {} will not receive anymore messages", name);
    }
    

    @Override
    public void onError(Throwable e)
    {
        log.info("Error received by subscriber {}", name, e);
    }

}
