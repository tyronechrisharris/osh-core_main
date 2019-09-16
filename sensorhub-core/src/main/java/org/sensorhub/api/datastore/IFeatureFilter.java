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
import java.util.function.Predicate;
import org.vast.ogc.gml.IFeature;


/**
 * <p>
 * Base interface for feature filters
 * </p>
 *
 * @author Alex Robin
 * @date Sep 7, 2019
 */
public interface IFeatureFilter extends IQueryFilter, Predicate<IFeature>
{
    
    public IdFilter getFeatureUIDs();


    public RangeFilter<Instant> getValidTime();


    public SpatialFilter getLocationFilter();
    
    
    public Predicate<FeatureKey> getKeyPredicate();


    public Predicate<IFeature> getValuePredicate();
    
    
    public boolean testFeatureUIDs(IFeature f);
    
    
    public boolean testValidTime(IFeature f);
    
    
    public boolean testLocation(IFeature f);
    
    
    public boolean testKeyPredicate(FeatureKey k);
    
    
    public boolean testValuePredicate(IFeature f);
}
