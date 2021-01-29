/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.sweapi;

import org.vast.util.Asserts;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


/**
 * <p>
 * Helper class to manage resource identifiers. We use this class to convert
 * from public facing resource IDs exposed via the API to internal OSH IDs.
 * </p><p>
 * We do this because internal IDs are often generated in sequence for
 * performance reason but we don't want to expose them to the outside for
 * security reasons (i.e. if IDs are assigned sequentially, one ID can be used
 * to infer the presence of another resource even if the user doesn't have
 * access to it, which facilitate certain kinds of cyber attacks).
 * </p><p>
 * In order to achieve some level of protection w/o requiring an ID mapping
 * table, we generate external IDs by including the internal ID as lower bits
 * and completing with a hash of the ID to generate a bigger number that
 * doesn't look sequential.
 * </p>
 *
 * @author Alex Robin
 * @date Nov 5, 2018
 */
public class IdEncoder
{
    static final long MAXID = 1L << 56;
    static final int INTERNAL_ID_SIZE_BITS = 6;
    static final int AVAILABLE_BITS = 63 - INTERNAL_ID_SIZE_BITS;
    static final int MIN_HASH_BITS = 8;
    static final int MIN_ENCODED_ID_BITS = 40 - INTERNAL_ID_SIZE_BITS;
    
    // 58 bits fits into 10 base58 digits
    // 40 bits fits into 10 hexadecimal digits (preferred?)
    // 39 bits = fits in 12 decimal digits
    // 36 bits fits into 11 decimal digits
    // 33 bits fits into 10 decimal digits
    
    HashFunction hf;
    
    
    public IdEncoder(int seed)
    {
        hf = Hashing.murmur3_32(seed);
    }
    
    
    public long encodeID(long internalID)
    {
        Asserts.checkArgument(internalID < MAXID, "ID cannot be longer than 56 bits");
        
        // number of bits necessary to encode ID
        long x = internalID;
        int numInternalIDBits = 0;
        while (x != 0)
        {
            numInternalIDBits++;
            x >>>= 1;
        }
        
        // use truncated hash of ID as higher part
        long hashValue = getHashValue(internalID, numInternalIDBits);
        int hashShift = numInternalIDBits + INTERNAL_ID_SIZE_BITS;
        
        return numInternalIDBits | (internalID << INTERNAL_ID_SIZE_BITS) | (hashValue << hashShift);
    }
    
    
    public long decodeID(long encodedID)
    {
        // read number of bytes used for ID part
        int numInternalIDBits = (int)(encodedID & 0x3F);
        long internalIDMask = (1L << numInternalIDBits) - 1;
        
        // extract ID part
        long internalID = (encodedID >>> INTERNAL_ID_SIZE_BITS) & internalIDMask;
        
        // check hash code
        long correctHashValue = getHashValue(internalID, numInternalIDBits);
        int hashShift = numInternalIDBits + INTERNAL_ID_SIZE_BITS;
        long embeddedHashValue = encodedID >>> hashShift;
        
        //Asserts.checkArgument(embeddedHashValue == correctHashValue, "Invalid external ID");
        if (embeddedHashValue != correctHashValue)
            return 0;
        else
            return internalID;
    }
    
    
    protected long getHashValue(long internalID, int numInternalIDBits)
    {
        HashCode hashcode = hf.hashLong(internalID);
        int numHashBits = getNumHashBits(numInternalIDBits);
        return (long)hashcode.asInt() & ((1L << numHashBits) - 1);
    }
    
    
    protected int getNumHashBits(int numInternalIDBits)
    {
        if (numInternalIDBits < (MIN_ENCODED_ID_BITS - MIN_HASH_BITS))
            return MIN_ENCODED_ID_BITS - numInternalIDBits;
        else
            return Math.min(MIN_HASH_BITS, AVAILABLE_BITS - numInternalIDBits);
    }
}
