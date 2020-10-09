/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.obs;

import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.obs.IDataStreamInfo;
import org.sensorhub.api.procedure.ProcedureId;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.swe.v20.DataComponent;
import net.opengis.swe.v20.DataEncoding;


/**
 * <p>
 * Base wrapper class for {@link IDataStreamInfo} objects
 * </p>
 *
 * @author Alex Robin
 * @date Oct 2, 2020
 */
public abstract class DataStreamInfoWrapper implements IDataStreamInfo
{
    IDataStreamInfo delegate;
    

    public DataStreamInfoWrapper(IDataStreamInfo dsInfo)
    {
        this.delegate = Asserts.checkNotNull(dsInfo, IDataStreamInfo.class);
    }
    
    
    @Override
    public ProcedureId getProcedureID()
    {
        return delegate.getProcedureID();
    }


    @Override
    public String getName()
    {
        return delegate.getName();
    }


    @Override
    public String getDescription()
    {
        return delegate.getDescription();
    }


    public int getRecordVersion()
    {
        return delegate.getRecordVersion();
    }


    public DataComponent getRecordStructure()
    {
        return delegate.getRecordStructure();
    }


    public DataEncoding getRecordEncoding()
    {
        return delegate.getRecordEncoding();
    }


    public TimeExtent getPhenomenonTimeRange()
    {
        return delegate.getPhenomenonTimeRange();
    }


    public TimeExtent getResultTimeRange()
    {
        return delegate.getResultTimeRange();
    }


    public boolean hasDiscreteResultTimes()
    {
        return delegate.hasDiscreteResultTimes();
    }


    public Map<Instant, TimeExtent> getDiscreteResultTimes()
    {
        return delegate.getDiscreteResultTimes();
    }
}
