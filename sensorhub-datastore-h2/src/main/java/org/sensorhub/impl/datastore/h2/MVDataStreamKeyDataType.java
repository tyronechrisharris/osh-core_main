/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.sensorhub.api.datastore.obs.DataStreamKey;


/**
 * <p>
 * H2 DataType implementation for internal DataStream key objects
 * </p>
 *
 * @author Alex Robin
 * @date Nov 3, 2020
 */
class MVDataStreamKeyDataType implements DataType
{
    private static final int MEM_SIZE = 8;
    
            
    @Override
    public int compare(Object objA, Object objB)
    {
        DataStreamKey a = (DataStreamKey)objA;
        DataStreamKey b = (DataStreamKey)objB;        
        return Long.compare(a.getInternalID(), b.getInternalID());
    }
    

    @Override
    public int getMemory(Object obj)
    {
        return MEM_SIZE;
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        DataStreamKey key = (DataStreamKey)obj;
        wbuf.putVarLong(key.getInternalID());
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
        long internalID = DataUtils.readVarLong(buff);       
        return new DataStreamKey(internalID);
    }
    

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);        
    }

}
