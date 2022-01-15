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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;


/**
 * <p>
 * H2 DataType implementation for internal command status key objects
 * </p>
 *
 * @author Alex Robin
 * @date Jan 5, 2022
 */
class MVCommandStatusKeyDataType implements DataType
{
    private static final int MEM_SIZE = 24;
    
    
    @Override
    public int compare(Object objA, Object objB)
    {
        MVCommandStatusKey a = (MVCommandStatusKey)objA;
        MVCommandStatusKey b = (MVCommandStatusKey)objB;
        
        // first compare command IDs
        int comp = a.cmdID.compareTo(b.cmdID);
        if (comp != 0)
            return comp;
        
        // if task IDs are equal, compare time stamps
        return a.reportTime.compareTo(b.reportTime);
    }
    

    @Override
    public int getMemory(Object obj)
    {
        return MEM_SIZE;
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        MVCommandStatusKey key = (MVCommandStatusKey)obj;
        byte[] cmdID = key.cmdID.toByteArray();
        wbuf.put((byte)cmdID.length);
        wbuf.put(cmdID);
        H2Utils.writeInstant(wbuf, key.reportTime);
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
        int cmdIdLen = buff.get();
        var cmdID = new BigInteger(buff.array(), buff.position(), cmdIdLen);
        buff.position(buff.position()+cmdIdLen);
        var reportTime = H2Utils.readInstant(buff);
        return new MVCommandStatusKey(cmdID, reportTime);
    }
    

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);
    }

}
