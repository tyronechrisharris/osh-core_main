/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.feature;

import org.sensorhub.api.datastore.EmptyFilterIntersection;
import org.vast.ogc.gml.IFeature;


/**
 * <p>
 * Immutable filter object for generic features.<br/>
 * There is an implicit AND between all filter parameters.
 * </p>
 *
 * @author Alex Robin
 * @date Apr 3, 2018
 */
public class FeatureFilter extends FeatureFilterBase<IFeature>
{        
    
    /*
     * this class can only be instantiated using builder
     */
    protected FeatureFilter()
    {
    }
    
    
    /**
     * Computes a logical AND between this filter and another filter of the same kind
     * @param filter The other filter to AND with
     * @return The new composite filter
     * @throws EmptyFilterIntersection if the intersection doesn't exist
     */
    public FeatureFilter and(FeatureFilter filter) throws EmptyFilterIntersection
    {
        if (filter == null)
            return this;
        return and(filter, new Builder()).build();
    }
    
    
    /*
     * Builder
     */
    public static class Builder extends FeatureFilterBuilder<Builder, IFeature, FeatureFilter>
    {
        public Builder()
        {
            super(new FeatureFilter());
        }
        
        public static Builder from(FeatureFilter base)
        {
            return new Builder().copyFrom(base);
        }
    }
}
