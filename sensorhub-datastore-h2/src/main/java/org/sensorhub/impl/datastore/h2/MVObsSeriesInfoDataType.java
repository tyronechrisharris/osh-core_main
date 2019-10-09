/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.h2;

import java.nio.ByteBuffer;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;


/**
 * <p>
 * H2 DataType implementation to serialize/deserialize ObsSeriesInfo objects
 * </p>
 *
 * @author Alex Robin
 * @date Sep 13, 2019
 */
public class MVObsSeriesInfoDataType implements DataType
{

    @Override
    public int compare(Object a, Object b)
    {
        // not used as index
        return 0;
    }


    @Override
    public int getMemory(Object obj)
    {
        MVObsSeriesInfo info = (MVObsSeriesInfo)obj;
        int memSize = 8 + 2;// + 2*info.procUID.length();
        if (info.foiUID != null)
            memSize += 2*info.foiUID.length();
        return memSize;
    }


    @Override
    public void write(WriteBuffer wbuf, Object obj)
    {
        MVObsSeriesInfo info = (MVObsSeriesInfo)obj;
        wbuf.putVarLong(info.id);
        //H2Utils.writeAsciiString(wbuf, info.procUID);
        H2Utils.writeAsciiString(wbuf, info.foiUID);
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
        long id = DataUtils.readVarLong(buff);
        //String procUID = H2Utils.readAsciiString(buff);
        String foiUID = H2Utils.readAsciiString(buff);
        return new MVObsSeriesInfo(id, foiUID);
    }


    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key)
    {
        for (int i=0; i<len; i++)
            obj[i] = read(buff);
    }

}
