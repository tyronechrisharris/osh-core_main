/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi.feature;

import java.time.Instant;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.feature.FeatureFilterBase;
import org.sensorhub.api.datastore.feature.FeatureKey;
import org.sensorhub.api.datastore.feature.IFeatureStoreBase;
import org.vast.ogc.gml.IFeature;
import com.google.common.collect.Iterators;


public class FeatureUtils
{

    public static <V extends IFeature, F extends FeatureFilterBase<? super V>, S extends IFeatureStoreBase<V,?,F>> FeatureKey getClosestKeyToNow(S dataStore, BigId id)
    {
        var s = dataStore.selectEntries(dataStore.filterBuilder()
                .withInternalIDs(id)
                .withAllVersions()
                .build());
        
        return keepOnlyClosestToNow(s)
            .map(e -> e.getKey())
            .findFirst()
            .orElse(null);
    }
    
    
    public static <V extends IFeature, F extends FeatureFilterBase<? super V>, S extends IFeatureStoreBase<V,?,F>> V getClosestToNow(S dataStore, BigId id)
    {
        var s = dataStore.selectEntries(dataStore.filterBuilder()
                .withInternalIDs(id)
                .withAllVersions()
                .build());
        
        return keepOnlyClosestToNow(s)
            .map(e -> e.getValue())
            .findFirst()
            .orElse(null);
    }
    
    
    public static <V extends IFeature> Stream<Entry<FeatureKey, V>> keepOnlyClosestToNow(Stream<Entry<FeatureKey, V>> results)
    {
        var now = Instant.now();
        var lastIdHolder = new AtomicLong();
        
        var pit = Iterators.peekingIterator(results.iterator());
        var it = Iterators.filter(pit, e -> {
            
            var lastId = lastIdHolder.get();
            var thisId = e.getKey().getInternalID().getIdAsLong();
            
            if (thisId != lastId && !pit.hasNext())
                return true;
            
            if (lastId == thisId)
                return false;
            
            var next = pit.peek();
            var nextId = next.getKey().getInternalID().getIdAsLong();
            var nextValidTime = next.getKey().getValidStartTime();
            if (thisId == nextId && nextValidTime.toEpochMilli() <= now.toEpochMilli())
                return false;
            
            lastIdHolder.set(thisId);
            return true;
        });
        
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.DISTINCT), false);
    }
}
