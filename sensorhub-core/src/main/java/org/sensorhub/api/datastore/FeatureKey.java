/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.time.Instant;
import java.util.Objects;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * Immutable key object used to index features in storage.
 * </p>
 *
 * @author Alex Robin
 * @since Mar 19, 2018
 */
public class FeatureKey
{    
    protected String uniqueID;
    protected Instant validStartTime = Instant.MIN;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected FeatureKey()
    {        
    }


    /**
     * @return The Unique ID of feature.<br/>
     * This cannot be null.
     */
    public String getUniqueID()
    {
        return uniqueID;
    }


    /**
     * @return The time range of validity of the feature description.<br/>
     * The time range must end with {@link Double.POSITIVE_INFINITY} if the end of validity is not yet known.<br/>
     * If null, the feature will always be returned by queries containing a temporal filter 
     */    
    public Instant getValidStartTime()
    {
        return validStartTime;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
    

    @Override
    public int hashCode()
    {
        return java.util.Objects.hash(
                getUniqueID(),
                getValidStartTime());
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof FeatureKey))
            return false;
        
        FeatureKey other = (FeatureKey)obj;
        return Objects.equals(getUniqueID(), other.getUniqueID()) &&
               Objects.equals(getValidStartTime(), other.getValidStartTime());
    }
    
    
    public static class Builder extends BaseBuilder<Builder, FeatureKey>
    {
        public Builder()
        {
            super(new FeatureKey());
        }
    }
    
    
    @SuppressWarnings("unchecked")
    protected abstract static class BaseBuilder<B extends BaseBuilder<B, T>, T extends FeatureKey> implements IBuilder<T>
    {
        protected T instance;


        protected BaseBuilder(T instance)
        {
            this.instance = instance;
        }


        public B withUniqueID(String uniqueID)
        {
            instance.uniqueID = uniqueID;
            return (B)this;
        }


        public B withValidStartTime(Instant start)
        {
            instance.validStartTime = start;
            return (B)this;
        }
        
        
        public T build()
        {
            Asserts.checkNotNull(instance.uniqueID, "uniqueID");
            Asserts.checkNotNull(instance.validStartTime, "validStartTime");
            
            T newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
