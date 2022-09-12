/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.system;

import java.util.Map;
import javax.xml.namespace.QName;
import org.sensorhub.api.procedure.IProcedureWithDesc;
import org.sensorhub.api.system.ISystemWithDesc;
import org.vast.ogc.gml.IFeature;
import org.vast.util.TimeExtent;
import net.opengis.sensorml.v20.AbstractProcess;


/**
 * <p>
 * Helper class to adapt a regular IFeature to the ISystemWithDesc interface
 * </p>
 *
 * @author Alex Robin
 * @since Jan 7, 2021
 */
public class SystemFeatureAdapter implements ISystemWithDesc, IProcedureWithDesc
{
    IFeature delegate;
    

    public SystemFeatureAdapter(IFeature f)
    {
        this.delegate = f;
    }
    
    
    public String getId()
    {
        return delegate.getId();
    }
    

    public String getUniqueIdentifier()
    {
        return delegate.getUniqueIdentifier();
    }
    

    public String getName()
    {
        return delegate.getName();
    }
    

    public Map<QName, Object> getProperties()
    {
        return delegate.getProperties();
    }
    

    public String getDescription()
    {
        return delegate.getDescription();
    }
    

    @Override
    public TimeExtent getValidTime()
    {
        return delegate.getValidTime();
    }
    

    @Override
    public AbstractProcess getFullDescription()
    {
        return null;
    }    
    
}
