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
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;


/**
 * <p>
 * H2 DataType implementation for FeatureKey objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVDataStreamProcKeyDataType implements DataType
{
    private static final int MIN_MEM_SIZE = 8+8+4;
    
            
    @Override
    public int compare(Object objA, Object objB)
    {
        MVDataStreamProcKey a = (MVDataStreamProcKey)objA;
        MVDataStreamProcKey b = (MVDataStreamProcKey)objB;
        
        // first compare procedure internal ID
        int comp = Long.compare(a.procedureID, b.procedureID);
        if (comp != 0)
            return comp;
        
        // only if IDs are the same, compare output name
        comp = a.outputName.compareTo(b.outputName);
        if (comp != 0)
            return comp;
        
        // only if output names are the same, compare version number
        // sort in reverse order so that latest version is always first
        return -Integer.compare(a.recordVersion, b.recordVersion);
    }
    

    @Override
    public int getMemory(Object obj)
    {
        MVDataStreamProcKey key = (MVDataStreamProcKey)obj;
        return MIN_MEM_SIZE + key.outputName.length();
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        MVDataStreamProcKey key = (MVDataStreamProcKey)obj;
        wbuf.putVarLong(key.internalID);
        wbuf.putVarLong(key.procedureID);
        H2Utils.writeAsciiString(wbuf, key.outputName);
        wbuf.putVarInt(key.recordVersion);
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
        long procID = DataUtils.readVarLong(buff);
        String outputName = H2Utils.readAsciiString(buff);
        int version = DataUtils.readVarInt(buff);        
        return new MVDataStreamProcKey(internalID, procID, outputName, version);
    }
    

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);        
    }

}
