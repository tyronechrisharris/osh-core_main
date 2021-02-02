/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.procedure.wrapper;

import java.time.Instant;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;
import net.opengis.sensorml.v20.IOPropertyList;


/**
 * <p>
 * Wrapper class for AbstractProcess implementing {@link IProcedureWithDesc}
 * and allowing to override outputs, parameters and validity time period.
 * </p>
 *
 * @author Alex Robin
 * @date Oct 4, 2020
 */
public class ProcedureWrapper implements IProcedureWithDesc
{
    ProcessWrapper<?> processWrapper;


    public ProcedureWrapper(AbstractProcess fullDesc)
    {
        Asserts.checkNotNull(fullDesc, AbstractProcess.class);
        
        if (fullDesc instanceof ProcessWrapper)
            this.processWrapper = (ProcessWrapper<?>)fullDesc;
        else
            this.processWrapper = ProcessWrapper.getWrapper(fullDesc);
    }
    
    
    public ProcedureWrapper hideOutputs()
    {
        processWrapper.withOutputs(new IOPropertyList());
        return this;
    }
    
    
    public ProcedureWrapper hideTaskableParams()
    {
        processWrapper.withParams(new IOPropertyList());
        return this;
    }
    
    
    /**
     * This methods sets valid time to be from 'currentTime' to 'now' (indeterminate)
     * in two cases:
     * - if no valid time was set at all
     * - if end of validity period was set to 'now' but begin is in the future. In this
     *   case, we override begin time stamp with current server time (not that this edge
     *   case can happen with a bad time sync on sender side and causes problems
     *   downstream when adding members and/or datastreams)
     * @return This wrapper for chaining
     */
    public ProcedureWrapper defaultToValidFromNow()
    {
        var validTime = processWrapper.getValidTime();
        var now = Instant.now();
        
        if (validTime == null || (validTime.endsNow() && validTime.begin().isAfter(now)))
            processWrapper.withValidTime(TimeExtent.endNow(Instant.now()));
        
        return this;
    }


    public ProcedureWrapper defaultToValidTime(TimeExtent validTime)
    {
        if (processWrapper.getValidTime() == null)
            processWrapper.withValidTime(validTime);
        return this;
    }


    @Override
    public String getId()
    {
        return processWrapper.getId();
    }
    
    
    @Override
    public String getUniqueIdentifier()
    {
        return processWrapper.getUniqueIdentifier();
    }


    @Override
    public String getName()
    {
        return processWrapper.getName();
    }


    @Override
    public String getDescription()
    {
        return processWrapper.getDescription();
    }
    

    @Override
    public TimeExtent getValidTime()
    {
        return processWrapper.getValidTime();
    }


    @Override
    public ProcessWrapper<?> getFullDescription()
    {
        return processWrapper;
    }
}
