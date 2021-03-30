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


/**
 * <p>
 * H2 DataType implementation for internal DataStream key objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVTimeSeriesProcKeyDataType implements DataType
{
    private static final int MIN_MEM_SIZE = 8+8+4;
    
            
    @Override
    public int compare(Object objA, Object objB)
    {
        MVTimeSeriesProcKey a = (MVTimeSeriesProcKey)objA;
        MVTimeSeriesProcKey b = (MVTimeSeriesProcKey)objB;
        
        // first compare procedure internal ID
        int comp = Long.compare(a.procedureID, b.procedureID);
        if (comp != 0)
            return comp;
        
        // only if IDs are the same, compare output name
        comp = a.signalName.compareTo(b.signalName);
        if (comp != 0)
            return comp;
        
        // only if output names are the same, compare valid times
        // sort in reverse order so that latest version is always first
        return -Long.compare(a.validStartTime, b.validStartTime);
    }
    

    @Override
    public int getMemory(Object obj)
    {
        MVTimeSeriesProcKey key = (MVTimeSeriesProcKey)obj;
        return MIN_MEM_SIZE + key.signalName.length();
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        MVTimeSeriesProcKey key = (MVTimeSeriesProcKey)obj;
        wbuf.putVarLong(key.internalID);
        wbuf.putVarLong(key.procedureID);
        H2Utils.writeAsciiString(wbuf, key.signalName);
        wbuf.putVarLong(key.validStartTime);
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
        long validStartTime = DataUtils.readVarLong(buff);        
        return new MVTimeSeriesProcKey(internalID, procID, outputName, validStartTime);
    }
    

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);        
    }

}
