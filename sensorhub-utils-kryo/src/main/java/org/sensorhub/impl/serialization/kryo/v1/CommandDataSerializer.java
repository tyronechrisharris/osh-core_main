/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.serialization.kryo.v1;

import org.sensorhub.api.command.CommandData;
import org.sensorhub.impl.serialization.kryo.BackwardCompatFieldSerializer;
import com.esotericsoftware.kryo.Kryo;


/**
 * <p>
 * Custom serializer to avoid serializing redundant ID data, backward
 * compatible with previous class format
 * </p>
 *
 * @author Alex Robin
 * @since May 3, 2022
 */
public class CommandDataSerializer extends BackwardCompatFieldSerializer<CommandData>
{

    public CommandDataSerializer(Kryo kryo, int idScope)
    {
        super(kryo, CommandData.class);
    }

}
