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
 * An ID provider that generates internal IDs based on computing a hash of
 * the feature unique ID.
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
        this.hashFunc = Hashing.murmur3_128(seed);
    }
    
    
    @Override
    public long newInternalID(IFeature f)
    {
        // We keep only 42-bits so it can fit on a 8-bytes DES encrypted block,
        // along with the ID scope and using variable length encoding.
        var hash = hashFunc.newHasher().putString(f.getUniqueIdentifier(), Charsets.UTF_8).hash();
        return hash.asLong() & 0x3FFFFFFFFFFL;
    }
}
