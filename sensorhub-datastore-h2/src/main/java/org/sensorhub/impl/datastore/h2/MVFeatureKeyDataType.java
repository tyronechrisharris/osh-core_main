/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2018 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Comparator;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;


/**
 * <p>
 * H2 DataType implementation for ObsKey objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVFeatureKeyDataType implements DataType
{
    private static final int MIN_MEM_SIZE = 10+14+2;
    Comparator<Instant> timeCompare = Comparator.nullsFirst(Comparator.naturalOrder());
    
            
    @Override
    public int compare(Object objA, Object objB)
    {
        MVFeatureKey a = (MVFeatureKey)objA;
        MVFeatureKey b = (MVFeatureKey)objB;
        
        // first compare internal ID part of the key, with special null cases
        int idComp = Long.compare(a.getInternalID(), b.getInternalID());
        if (idComp != 0)
            return idComp;
        
        // only if IDs are the same, compare timeStamp part
        return timeCompare.compare(a.getValidStartTime(), b.getValidStartTime());
    }
    

    @Override
    public int getMemory(Object obj)
    {
        MVFeatureKey key = (MVFeatureKey)obj;
        return MIN_MEM_SIZE + key.getUniqueID().length();
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        MVFeatureKey key = (MVFeatureKey)obj;
        
        // internal ID
        wbuf.putVarLong(key.getInternalID());
        
        // time stamp
        H2Utils.writeInstant(wbuf, key.getValidStartTime());
        
        // unique ID
        String uid = key.getUniqueID();
        H2Utils.writeAsciiString(wbuf, uid, uid.length());
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            write(wbuf, obj[i]);
    }
    

    @Override
    public Object read(ByteBuffer buff)
    {
        // internal ID
        long internalID = DataUtils.readVarLong(buff); 
        
        // start time stamp
        Instant startTime = H2Utils.readInstant(buff);
        
        // unique ID
        String uid = H2Utils.readAsciiString(buff);
        
        return new MVFeatureKey.Builder()
                .withInternalID(internalID)
                .withUniqueID(uid)
                .withValidStartTime(startTime)
                .build();
    }
    

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);        
    }

}
