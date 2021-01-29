/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2021 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.kryo.v1;

import java.time.Instant;
import org.vast.util.TimeExtent;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


/**
 * <p>
 * Helper methods to serialize/deserialize common objects to/from Kryo
 * Output/Input buffer classes.
 * </p>
 *
 * @author Alex Robin
 * @since Jan 29, 2021
 */
public class KryoUtils
{
    static final String UNKNOWN_TIME_ERROR = "Unsupported time extent type: ";
    static enum TimeExtentType {NULL, NOW, END_NOW, BEGIN_NOW, INSTANT, PERIOD}

    
    public static void writeTimeExtent(Output output, TimeExtent te)
    {
        // write flag + begin and/or end depending on time extent type
        if (te == null)
        {
            output.writeByte(TimeExtentType.NULL.ordinal());
        }
        else if (te.isNow())
        {
            output.writeByte(TimeExtentType.NOW.ordinal());
        }
        else if (te.endsNow())
        {
            output.writeByte(TimeExtentType.END_NOW.ordinal());
            writeInstant(output, te.begin());
        }
        else if (te.beginsNow())
        {
            output.writeByte(TimeExtentType.BEGIN_NOW.ordinal());
            writeInstant(output, te.end());
        }
        else if (te.isInstant())
        {
            output.writeByte(TimeExtentType.INSTANT.ordinal());
            writeInstant(output, te.begin());
        }
        else
        {
            output.writeByte(TimeExtentType.PERIOD.ordinal());
            writeInstant(output, te.begin());
            writeInstant(output, te.end());
        }
    }
    
    
    public static TimeExtent readTimeExtent(Input input)
    {
        int flag = input.readByte();
        if (flag > TimeExtentType.values().length)
            throw new IllegalStateException(UNKNOWN_TIME_ERROR + flag);
        
        Instant begin, end;
        var timeType = TimeExtentType.values()[flag];
        switch (timeType)
        {
            case NULL:
                return null;
                
            case NOW:
                return TimeExtent.now();
            
            case END_NOW:
                begin = readInstant(input);
                return TimeExtent.endNow(begin);
        
            case BEGIN_NOW:
                end = readInstant(input);
                return TimeExtent.beginNow(end);
        
            case INSTANT:
                begin = readInstant(input);
                return TimeExtent.instant(begin);
        
            case PERIOD:
                begin = readInstant(input);
                end = readInstant(input);
                return TimeExtent.period(begin, end);
                
            default:
                throw new IllegalStateException(UNKNOWN_TIME_ERROR + timeType);
        }
    }
    
    
    protected static void writeInstant(Output output, Instant instant)
    {
        output.writeVarLong(instant.getEpochSecond(), false);
        output.writeInt(instant.getNano());
    }
    
    
    public static Instant readInstant(Input input)
    {
        long epochSeconds = input.readVarLong(false);
        int nanos = input.readInt();
        
        if (epochSeconds == Instant.MIN.getEpochSecond() && nanos == 0)
            return Instant.MIN;
        else if (epochSeconds == Instant.MAX.getEpochSecond() && nanos == 0)
            return Instant.MAX;
        else
            return Instant.ofEpochSecond(epochSeconds, nanos);
    }

}
