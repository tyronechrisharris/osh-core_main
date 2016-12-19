/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2012-2016 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.swe;

import java.util.ArrayList;
import org.vast.util.Asserts;


public class OfferingList<T extends OfferingConfig> extends ArrayList<T>
{        
    private static final long serialVersionUID = 3446250829299706031L;

    
    public void replaceOrAdd(T newConfig)
    {
        Asserts.checkNotNull(newConfig.offeringID);
        
        // find config with same uri
        int index = -1;
        for (int i = 0; i < size(); i++)
        {
            if (newConfig.offeringID.equals(get(i).offeringID))
            {
                index = i;
                break;
            }
        }
        
        // replace by new config
        if (index >= 0)
            set(index, newConfig);
        else
            add(newConfig);
    }
}