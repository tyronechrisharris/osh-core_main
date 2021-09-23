/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import org.vast.ogc.gml.IFeature;
import org.vast.ogc.gml.IGeoFeature;
import org.vast.ogc.gml.ITemporalFeature;
import org.vast.util.Asserts;
import org.vast.util.TimeExtent;
import net.opengis.gml.v32.AbstractGeometry;


/**
 * <p>
 * Wrapper object so that all features are serialized generically without
 * storing the actual implementation class names which may not be available
 * when deserializing from storage.
 * </p>
 *
 * @author Alex Robin
 * @since Sep 21, 2021
 */
public class FeatureWrapper implements IGeoFeature, ITemporalFeature
{
    IFeature f;
    
    FeatureWrapper(IFeature f)
    {
        this.f = Asserts.checkNotNull(f);
    }
    
    @Override
    public String getId()
    {
        return f.getId();
    }

    @Override
    public String getUniqueIdentifier()
    {
        return f.getUniqueIdentifier();
    }

    @Override
    public String getName()
    {
        return f.getName();
    }

    @Override
    public String getDescription()
    {
        return f.getDescription();
    }

    @Override
    public TimeExtent getValidTime()
    {
        if (f instanceof ITemporalFeature)
            return ((ITemporalFeature) f).getValidTime();
        else
            return null;
    }

    @Override
    public AbstractGeometry getGeometry()
    {
        if (f instanceof IGeoFeature)
            return ((IGeoFeature) f).getGeometry();
        else
            return null;
    }        
}