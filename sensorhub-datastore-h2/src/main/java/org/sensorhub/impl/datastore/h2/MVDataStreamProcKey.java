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

import org.sensorhub.utils.ObjectUtils;

/**
 * <p>
 * Immutable key object used to identify an individual data stream.
 * </p>
 *
 * @author Alex Robin
 * @since Sep 19, 2019
 */
class MVDataStreamProcKey
{    
    long internalID;
    long procedureID;
    String outputName;
    int recordVersion;
    
    
    MVDataStreamProcKey(long procID, String outputName, int version)
    {        
        this(0, procID, outputName, version);
    }
    
    
    MVDataStreamProcKey(long internalID, long procID, String outputName, int version)
    {        
        this.internalID = internalID;
        this.procedureID = procID;
        this.outputName = outputName;
        this.recordVersion = version;
    }


    @Override
    public String toString()
    {
        return ObjectUtils.toString(this, true);
    }
}
