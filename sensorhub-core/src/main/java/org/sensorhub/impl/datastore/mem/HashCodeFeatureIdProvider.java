/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are copyright (C) 2018, Sensia Software LLC
 All Rights Reserved. This software is the property of Sensia Software LLC.
 It cannot be duplicated, used, or distributed without the express written
 consent of Sensia Software LLC.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.mem;

import org.sensorhub.api.datastore.IdProvider;
import org.vast.ogc.gml.IFeature;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


/**
 * <p>
 * An ID provider that generates internal IDs based on computing the hash
 * code of a feature unique ID
 * </p>
 *
 * @author Alex Robin
 * @date Nov 12, 2020
 */
public class HashCodeFeatureIdProvider implements IdProvider<IFeature>
{
    HashFunction hashFunc;
    
    
    public HashCodeFeatureIdProvider(int seed)
    {
        hashFunc = Hashing.murmur3_128(seed);
    }
    
    
    @Override
    public long newInternalID(IFeature f)
    {
        //var hash = Objects.hash(f.getUniqueIdentifier());
        //return hash & 0xFFFFFFFFL;
        var hash = hashFunc.newHasher().putString(f.getUniqueIdentifier(), Charsets.UTF_8).hash();
        return hash.asInt() & 0xFFFFFFFFL;
    }
}
