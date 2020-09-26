/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.registry;

import java.time.Instant;
import java.util.Map;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.api.obs.IObsData;
import com.vividsolutions.jts.geom.Geometry;
import net.opengis.swe.v20.DataBlock;


/**
 * <p>
 * IObsData delegate used to override behavior of an existing IObsData
 * implementation. 
 * </p>
 *
 * @author Alex Robin
 * @date Mar 24, 2020
 */
public class ObsDelegate implements IObsData
{
    IObsData delegate;


    public ObsDelegate(IObsData obs)
    {
        this.delegate = obs;
    }
    
    
    public long getDataStreamID()
    {
        return delegate.getDataStreamID();
    }


    public FeatureId getFoiID()
    {
        return delegate.getFoiID();
    }


    public Instant getPhenomenonTime()
    {
        return delegate.getPhenomenonTime();
    }


    public Instant getResultTime()
    {
        return delegate.getResultTime();
    }


    public Map<String, Object> getParameters()
    {
        return delegate.getParameters();
    }


    public Geometry getPhenomenonLocation()
    {
        return delegate.getPhenomenonLocation();
    }


    public DataBlock getResult()
    {
        return delegate.getResult();
    }
}
