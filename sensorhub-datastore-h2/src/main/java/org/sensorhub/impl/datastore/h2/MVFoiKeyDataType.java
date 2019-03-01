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
import org.sensorhub.api.datastore.ObsKey;


/**
 * <p>
 * H2 DataType implementation for ObsKey objects
 * </p>
 *
 * @author Alex Robin
 * @date Apr 7, 2018
 */
class MVFoiKeyDataType implements DataType
{
    private static final int MIN_MEM_SIZE = 2+12+12;
    Comparator<String> idCompare = Comparator.nullsFirst(Comparator.naturalOrder());
    
    
    @Override
    public int compare(Object objA, Object objB)
    {
        ObsKey a = (ObsKey)objA;
        ObsKey b = (ObsKey)objB;
        
        // first compare ID part of the key
        int stringComp = idCompare.compare(a.getProcedureID(), b.getProcedureID());
        if (stringComp != 0)
            return stringComp;
        
        // only if IDs are the same, compare timeStamp part
        return a.getPhenomenonTime().compareTo(b.getPhenomenonTime());
    }
    

    @Override
    public int getMemory(Object obj)
    {
        ObsKey key = (ObsKey)obj;
        if (key.getProcedureID() == null)
            return MIN_MEM_SIZE;
        else
            return MIN_MEM_SIZE + key.getProcedureID().length()*2;
    }
    

    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        ObsKey key = (ObsKey)obj;
        String producerID = key.getProcedureID();
        
        if (producerID != null)
        {
            short idLength = (short)producerID.length();
            wbuf.putShort(idLength);
            wbuf.putStringData(producerID, idLength);
        }
        else
            wbuf.putShort((short)0);
        
        wbuf.putLong(key.getPhenomenonTime().getEpochSecond());
        wbuf.putInt(key.getPhenomenonTime().getNano());
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
        String procedureID = null;
        int idLength = buff.getShort();
        if (idLength > 0)
            procedureID = DataUtils.readString(buff, idLength);
        
        long epochSeconds = buff.getLong();
        int nanos = buff.getInt();
        return ObsKey.builder()
                .withProcedureID(procedureID)
                .withPhenomenonTime(Instant.ofEpochSecond(epochSeconds, nanos))
                .build();
    }
    

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);        
    }

}
