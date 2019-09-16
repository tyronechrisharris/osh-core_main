/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.datastore;

import java.time.Instant;
import java.util.Objects;
import org.sensorhub.utils.ObjectUtils;
import org.vast.util.Asserts;


/**
 * <p>
 * Immutable key object used to index observations in storage.
 * </p>
 *
 * @author Alex Robin
 * @since Mar 19, 2018
 */
public class ObsKey
{    
    public static final FeatureKey NO_FOI;
    static { NO_FOI = new FeatureKey(); NO_FOI.internalID = 0; }
    
    protected FeatureKey procedureKey = null;
    protected FeatureKey foiKey = NO_FOI;
    protected Instant phenomenonTime = null;
    protected Instant resultTime = null;
    
    
    /*
     * this class can only be instantiated using builder
     */
    protected ObsKey()
    {
    }


    /**
     * @return The feature key for the procedure that g the observation.<br/>
     * This field cannot be null.
     */
    public FeatureKey getProcedureKey()
    {
        return procedureKey;
    }


    /**
     * @return The feature key for the FOI that was observed.<br/>
     * This field cannot be null.
     */
    public FeatureKey getFoiKey()
    {
        return foiKey;
    }


    /**
     * @return The time of occurrence of the measured phenomenon (e.g. for
     * many automated sensor devices, this is typically the sampling time).<br/>
     * This field cannot be null.
     */
    public Instant getPhenomenonTime()
    {
        return phenomenonTime;
    }


    /**
     * @return The time at which the observation result was obtained.<br/>
     * This is typically the same as the phenomenon time for many automated
     * in-situ and remote sensors doing the sampling and actual measurement
     * (almost) simultaneously, but different for measurements made in a lab on
     * samples that were collected previously. It is also different for models
     * and simulations outputs (e.g. for a model, this is the run time).<br/>
     * If no result time was explicitly set, this returns the phenomenon time
     */
    public Instant getResultTime()
    {
        if (resultTime == null || resultTime == Instant.MIN)
            return phenomenonTime;
        return resultTime;
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
                getProcedureKey(),
                getFoiKey(),
                getPhenomenonTime(),
                getResultTime());
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || !(obj instanceof ObsKey))
            return false;
        
        ObsKey other = (ObsKey)obj;
        return Objects.equals(getProcedureKey(), other.getProcedureKey()) &&
               Objects.equals(getFoiKey(), other.getFoiKey()) &&
               Objects.equals(getPhenomenonTime(), other.getPhenomenonTime()) &&
               Objects.equals(getResultTime(), other.getResultTime());
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T extends Builder> T builder()
    {
        return (T)new Builder(new ObsKey());
    }
    
    
    @SuppressWarnings("unchecked")
    public static class Builder<B extends Builder<B, T>, T extends ObsKey> implements IBuilder<T>
    {
        protected T instance;


        protected Builder(T instance)
        {
            this.instance = instance;
        }


        public B withProcedureKey(FeatureKey procedureKey)
        {
            instance.procedureKey = procedureKey;
            return (B)this;
        }


        public B withFoiKey(FeatureKey foiKey)
        {
            instance.foiKey = foiKey;
            return (B)this;
        }
        

        public B withPhenomenonTime(Instant phenomenonTime)
        {
            instance.phenomenonTime = phenomenonTime;
            return (B)this;
        }


        public B withResultTime(Instant resultTime)
        {
            instance.resultTime = resultTime;
            return (B)this;
        }
        
        
        public T build()
        {
            Asserts.checkNotNull(instance.procedureKey, "procedureKey");
            Asserts.checkNotNull(instance.foiKey, "foiKey");
            Asserts.checkNotNull(instance.phenomenonTime, "phenomenonTime");
            
            T newInstance = instance;
            instance = null; // nullify instance to prevent further changes
            return newInstance;
        }
    }
}
