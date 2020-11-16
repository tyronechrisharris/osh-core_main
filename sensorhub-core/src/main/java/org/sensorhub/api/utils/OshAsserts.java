/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2019 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.utils;

import org.vast.util.Asserts;


public class OshAsserts
{
    static final int UID_MIN_CHARS = 8; 
    
    
    public static String checkValidUID(String uid)
    {
        Asserts.checkNotNull(uid, "UniqueID");
        Asserts.checkArgument(uid.length() > UID_MIN_CHARS, "Unique ID must be at least " + UID_MIN_CHARS + " characters long");
        return uid;
    }
    
    
    public static long checkValidInternalID(long id)
    {
        Asserts.checkArgument(id > 0, "Internal ID must be > 0");
        return id;
    }
}
