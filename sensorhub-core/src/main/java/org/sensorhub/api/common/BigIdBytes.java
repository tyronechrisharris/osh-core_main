/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.api.common;

import java.util.Arrays;
import org.vast.util.Asserts;
import com.google.common.io.BaseEncoding;


public class BigIdBytes implements BigId
{
    int scope;
    byte[] id;
    
    
    public BigIdBytes(int scope, byte[] id)
    {
        this.scope = scope;
        this.id = Asserts.checkNotNullOrEmpty(id, "id");
    }


    @Override
    public int getScope()
    {
        return scope;
    }


    @Override
    public byte[] getIdAsBytes()
    {
        return id;
    }


    @Override
    public int size()
    {
        return id.length;
    }


    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof BigId))
            return false;
        
        return BigId.equals(this, (BigId)obj);
    }


    @Override
    public int hashCode()
    {
        return 31*Integer.hashCode(scope) + Arrays.hashCode(id);
    }


    @Override
    public String toString()
    {
        return String.format("BigId {scope=%u, id=%s}", scope,
               BaseEncoding.base32Hex().lowerCase().encode(id));
    }
}