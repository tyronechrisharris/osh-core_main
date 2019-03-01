/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.time.ZoneOffset;
import org.sensorhub.api.datastore.IBuilder;
import org.vast.util.Asserts;

/**
 * <p>
 * Data structure for storing data store information
 * </p>
 *
 * @author Alex Robin
 * @date Apr 5, 2018
 */
public class MVDataStoreInfo
{
    protected String name;
    protected ZoneOffset zoneOffset = ZoneOffset.UTC;
    

    protected MVDataStoreInfo()
    {        
    }
    
    
    public String getName()
    {
        return name;
    }


    public ZoneOffset getZoneOffset()
    {
        return zoneOffset;
    }
    
    
    public static class Builder extends BaseBuilder<Builder, MVDataStoreInfo>
    {
        public Builder()
        {
            super(new MVDataStoreInfo());
        }        
    }
    
    
    @SuppressWarnings("unchecked")
    protected abstract static class BaseBuilder<B extends BaseBuilder<B, T>, T extends MVDataStoreInfo> implements IBuilder<T>
    {
        protected T instance;


        protected BaseBuilder(T instance)
        {
            this.instance = instance;
        }


        public B withName(String name)
        {
            instance.name = name;
            return (B)this;
        }


        public B withTimeZone(ZoneOffset zoneOffset)
        {
            instance.zoneOffset = zoneOffset;
            return (B)this;
        }
        
        
        public T build()
        {
            Asserts.checkNotNull(instance.name, "name");
            return instance;
        }
    }
}
