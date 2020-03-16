/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2020 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.swe;

import java.util.List;
import org.vast.util.Asserts;


/**
 * <p>
 * Uility methods to update offerings
 * </p>
 *
 * @author Alex Robin
 * @date Mar 16, 2020
 */
public class OfferingUtils
{

    public static <T extends OfferingConfig> void replaceOrAddOfferingConfig(List<T> configList, T newConfig)
    {
        Asserts.checkNotNull(newConfig.offeringID);
        
        // find config with same uri
        int index = -1;
        for (int i = 0; i < configList.size(); i++)
        {
            String existingOfferingID = configList.get(i).offeringID;
            if (newConfig.offeringID.equals(existingOfferingID))
            {
                index = i;
                break;
            }
        }
        
        // replace by new config
        if (index >= 0)
            configList.set(index, newConfig);
        else
            configList.add(newConfig);
    }
}
